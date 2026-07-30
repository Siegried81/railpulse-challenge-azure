import sys
import csv
import os
import pyodbc


def get_db_connection():
    conn_str = os.environ.get("SQL_CONNECTION_STRING")
    if not conn_str:
        raise EnvironmentError(
            "SQL_CONNECTION_STRING is not set.\n"
            "Set it in PowerShell before running this script:\n"
            '  $env:SQL_CONNECTION_STRING = "Driver={ODBC Driver 18 for SQL Server};'
            'Server=tcp:serv-railpulse.database.windows.net,1433;'
            'Database=db-railpulse;Uid=apiuser;Pwd=<ton_mot_de_passe>;'
            'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"'
        )
    return pyodbc.connect(conn_str)


def parse_optional_int(raw: str):
    """GTFS enum fields (wheelchair_boarding, location_type) are optional;
    empty string means 'no information' and should be stored as NULL."""
    raw = raw.strip()
    return int(raw) if raw else None


def load_stations(stops_path: str, cursor) -> tuple[int, int]:
    inserted = 0
    skipped = 0

    with open(stops_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            station_id = row.get("stop_id", "").strip()[:20]
            name = row.get("stop_name", "").strip()

            if not station_id or not name:
                skipped += 1
                continue

            lat_raw = row.get("stop_lat", "").strip()
            lon_raw = row.get("stop_lon", "").strip()
            lat = float(lat_raw) if lat_raw else None
            lon = float(lon_raw) if lon_raw else None

            standard_name = name

            wheelchair_boarding = parse_optional_int(row.get("wheelchair_boarding", ""))
            location_type = parse_optional_int(row.get("location_type", ""))

            cursor.execute("""
                MERGE stations AS target
                USING (SELECT ? AS station_id) AS source
                ON target.station_id = source.station_id
                WHEN MATCHED THEN
                    UPDATE SET name = ?, standard_name = ?, latitude = ?, longitude = ?,
                               wheelchair_boarding = ?, location_type = ?
                WHEN NOT MATCHED THEN
                    INSERT (station_id, name, standard_name, latitude, longitude,
                            wheelchair_boarding, location_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
            """, station_id,
                 name, standard_name, lat, lon, wheelchair_boarding, location_type,
                 station_id, name, standard_name, lat, lon, wheelchair_boarding, location_type)

            inserted += 1

    return inserted, skipped


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/load_stations.py data/stops.txt")
        sys.exit(1)

    stops_path = sys.argv[1]

    if not os.path.isfile(stops_path):
        print(f"File not found: {stops_path}")
        sys.exit(1)

    conn = get_db_connection()
    cursor = conn.cursor()

    print(f"Loading stations from {stops_path} ...")
    inserted, skipped = load_stations(stops_path, cursor)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Done — {inserted} stations upserted, {skipped} rows skipped (missing id/name).")


if __name__ == "__main__":
    main()