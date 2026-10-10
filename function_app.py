import os
import json
import logging
import datetime
import uuid
import requests
import pyodbc
import azure.functions as func

# Initialize Azure Functions application instance
app = func.FunctionApp()

# API endpoint URLs for SNCB/NMBS GTFS-Realtime feeds
TRIP_UPDATES_URL = "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/nmbssncb/rt/trip-update"
ALERTS_URL = "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/nmbssncb/rt/alert"

# GTFS-Realtime schedule relationship code for canceled trips
TRIP_SCHEDULE_RELATIONSHIP_CANCELED = 3  # 0=Scheduled, 1=Added, 2=Unscheduled, 3=Canceled

# Major Belgian rail hubs tracked for network statistics
MAJOR_HUBS = [
    "Bruxelles-Central",
    "Anvers-Central",
    "Gand-Saint-Pierre",
    "Charleroi-Central",
    "Liège-Guillemins",
]

# Common Cross-Origin Resource Sharing headers for HTTP responses
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def get_db_connection():
    """Retrieve SQL connection string from environment variables and open a pyodbc connection."""
    connection_string = os.environ.get("SQL_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError(
            "SQL_CONNECTION_STRING is not set. "
            "Configure it in Application Settings (portal) or local.settings.json (local)."
        )
    return pyodbc.connect(connection_string)


def fetch_gtfs_rt_feed(url: str) -> dict:
    """Fetch and return GTFS-Realtime data as JSON from the specified feed URL."""
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def normalize_station_id(raw_stop_id: str) -> str:
    """Normalize raw stop ID to match the station primary key format."""
    base_id = raw_stop_id.split("_")[0]
    return base_id[:20]


def load_known_station_ids(cursor) -> set:
    """
    Fetch every station_id already loaded once, instead of running a
    SELECT per stopTimeUpdate. A single feed pull can contain thousands of
    stopTimeUpdate entries, so this turns O(n) DB round-trips into O(1).
    """
    cursor.execute("SELECT station_id FROM stations")
    return {row.station_id for row in cursor.fetchall()}


def upsert_vehicle(cursor, vehicle_id: str, vehicle_type: str, direction: str):
    """Insert a new vehicle record or update an existing one safely in the database."""
    cursor.execute("""
        IF EXISTS (SELECT 1 FROM vehicles WHERE vehicle_id = ?)
            UPDATE vehicles 
            SET vehicle_type = COALESCE(NULLIF(?, 'NA'), vehicle_type), 
                direction = COALESCE(NULLIF(?, ''), direction)
            WHERE vehicle_id = ?
        ELSE
            INSERT INTO vehicles (vehicle_id, vehicle_type, direction)
            VALUES (?, ?, ?)
    """, vehicle_id, vehicle_type, direction, vehicle_id, vehicle_id, vehicle_type, direction)


def insert_liveboard_record(cursor, conn, station_id, vehicle_id, platform,
                            scheduled_time, delay_seconds, canceled, pulled_at) -> bool:
    """Insert one liveboard record and report whether a row was actually written.

    The duplicate check is part of the statement (`WHERE NOT EXISTS` on the
    columns of UQ_liveboard_dedup) rather than a caught IntegrityError followed
    by `conn.rollback()`. A rollback on the shared connection undoes every row
    the current pull had inserted so far, not just the duplicate: one repeated
    stop time could wipe a whole pull while the caller kept counting. The
    IntegrityError branch stays only for the race where another run inserted
    the same key between the check and the insert; it skips the row and leaves
    the transaction alone.

    `cursor.rowcount` is the truth about persistence, so the caller can count
    rows written instead of rows attempted.
    """
    try:
        cursor.execute("""
            INSERT INTO liveboard_records
                (station_id, vehicle_id, platform, scheduled_time, delay_seconds, canceled, pulled_at)
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM liveboard_records
                WHERE station_id = ? AND vehicle_id = ? AND scheduled_time = ? AND pulled_at = ?
            )
        """, station_id, vehicle_id, platform, scheduled_time, delay_seconds, canceled, pulled_at,
             station_id, vehicle_id, scheduled_time, pulled_at)
        return cursor.rowcount == 1
    except pyodbc.IntegrityError:
        logging.warning(
            f"Duplicate entry skipped: "
            f"station={station_id}, vehicle={vehicle_id}, scheduled_time={scheduled_time}, pulled_at={pulled_at}"
        )
        return False


def process_trip_updates(cursor, conn) -> int:
    """Iterate through trip update entities from the GTFS-RT feed and ingest valid records."""
    feed = fetch_gtfs_rt_feed(TRIP_UPDATES_URL)
    pulled_at = datetime.datetime.now(datetime.timezone.utc)
    count = 0
    seen_keys = set()
    known_station_ids = load_known_station_ids(cursor)

    for entity in feed.get("entity", []):
        trip_update = entity.get("tripUpdate")
        if not trip_update:
            continue

        trip = trip_update.get("trip", {})
        vehicle_id = trip.get("tripId", "UNKNOWN")
        canceled = 1 if trip.get("scheduleRelationship") == TRIP_SCHEDULE_RELATIONSHIP_CANCELED else 0

        # Extract vehicle_type and direction from the feed if available, using fallbacks otherwise
        vehicle_type = trip.get("vehicleType", "Train")
        direction = str(trip.get("directionId", ""))

        upsert_vehicle(cursor, vehicle_id, vehicle_type, direction)

        for stu in trip_update.get("stopTimeUpdate", []):
            raw_stop_id = stu.get("stopId")
            if not raw_stop_id:
                continue

            station_id = normalize_station_id(raw_stop_id)
            if station_id not in known_station_ids:
                continue

            departure = stu.get("departure")
            arrival = stu.get("arrival")

            if departure and departure.get("time"):
                scheduled_time = datetime.datetime.fromtimestamp(departure["time"], tz=datetime.timezone.utc)
                delay_seconds = departure.get("delay", 0)
            elif arrival and arrival.get("time"):
                scheduled_time = datetime.datetime.fromtimestamp(arrival["time"], tz=datetime.timezone.utc)
                delay_seconds = arrival.get("delay", 0)
            else:
                continue

            dedup_key = (station_id, vehicle_id, scheduled_time)
            if dedup_key in seen_keys:
                logging.info(f"In-memory duplicate skipped before insert: {dedup_key}")
                continue
            seen_keys.add(dedup_key)

            # Count rows persisted, not rows attempted: the response says
            # "N records ingested" and that figure has to be the one in the table.
            if insert_liveboard_record(
                cursor, conn, station_id, vehicle_id, "",
                scheduled_time, delay_seconds, canceled, pulled_at
            ):
                count += 1

    return count


def run_ingestion() -> int:
    """
    Shared by both the HTTP-triggered and timer-triggered ingestion entry
    points so the connect/commit/close lifecycle only exists in one place.
    The connection is always closed via `finally`, even if an exception
    happens mid-pull — otherwise a failed run leaks a SQL connection.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        total = process_trip_updates(cursor, conn)
        conn.commit()
        cursor.close()
        return total
    finally:
        conn.close()


def _json_response(payload, status_code: int = 200) -> func.HttpResponse:
    """Shared JSON + CORS response builder for the read-only API endpoints."""
    return func.HttpResponse(
        json.dumps(payload),
        status_code=status_code,
        mimetype="application/json",
        headers=CORS_HEADERS,
    )


def _log_failure(what: str, exc: Exception) -> str:
    """Log the full exception server-side and return a correlation id for the client.

    The raw exception text used to be the HTTP body. For a database error that
    is the driver's message, which names the server, the table and sometimes
    the failing value - useful to an operator reading the logs, not something
    to hand to an anonymous caller. The id lets the two be matched.
    """
    correlation_id = uuid.uuid4().hex[:12]
    logging.error(f"{what} failed [{correlation_id}]: {exc}")
    return correlation_id


# Writes need a key: the ingestion pulls the SNCB feed and inserts into Azure
# SQL, so an anonymous route let anyone on the internet trigger paid work and
# fill the table at will. FUNCTION level means the caller presents the
# function key (`?code=...`); the timer trigger below needs no key and is how
# ingestion normally runs. POST only, because a GET that writes is the request
# a link preview or a crawler makes by accident.
@app.route(route="fetch-liveboard", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def fetch_liveboard_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint to trigger manual execution of the GTFS-RT ingestion pipeline."""
    logging.info("HTTP trigger: fetching GTFS-RT trip updates")
    try:
        total = run_ingestion()
        return func.HttpResponse(
            f"OK — {total} liveboard records ingested from GTFS-RT feed.",
            status_code=200
        )
    except Exception as e:
        correlation_id = _log_failure("Ingestion", e)
        return func.HttpResponse(
            f"Ingestion failed. Reference {correlation_id}.", status_code=500
        )


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False)
def fetch_liveboard_timer(mytimer: func.TimerRequest) -> None:
    """Timer trigger executing the ingestion pipeline automatically every 15 minutes."""
    logging.info("Timer trigger: fetching GTFS-RT trip updates")
    try:
        total = run_ingestion()
        logging.info(f"Timer run complete — {total} records ingested.")
    except Exception as e:
        logging.error(f"Timer ingestion failed: {e}")


@app.route(route="station-stats", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET", "OPTIONS"])
def station_stats_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP API endpoint returning the top 5 most active stations by record count."""
    logging.info("HTTP trigger: fetching top-5 stations by record count")

    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=204, headers=CORS_HEADERS)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 5 s.name AS station_name, COUNT(*) AS record_count
            FROM liveboard_records lr
            JOIN stations s ON lr.station_id = s.station_id
            GROUP BY s.name
            ORDER BY record_count DESC;
        """)
        rows = cursor.fetchall()
        result = [{"station_name": row.station_name, "record_count": row.record_count} for row in rows]
        cursor.close()
        return _json_response(result)
    except Exception as e:
        correlation_id = _log_failure("Station stats query", e)
        return _json_response(
            {"error": "query failed", "reference": correlation_id}, status_code=500
        )
    finally:
        if conn is not None:
            conn.close()


@app.route(route="network-leaderboard", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET", "OPTIONS"])
def network_leaderboard_http(req: func.HttpRequest) -> func.HttpResponse:
    """Return punctuality figures for the five main Belgian rail hubs."""
    logging.info("HTTP trigger: fetching network leaderboard data")

    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=204, headers=CORS_HEADERS)

    placeholders = ", ".join("?" for _ in MAJOR_HUBS)
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                s.name AS station_name,
                COUNT(*) AS total_records,
                ROUND(
                    SUM(CASE WHEN lr.canceled = 0 AND lr.delay_seconds <= 60 THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN lr.canceled = 0 THEN 1 ELSE 0 END), 0),
                    2
                ) AS on_time_pct,
                AVG(CASE WHEN lr.canceled = 0 THEN CAST(lr.delay_seconds AS FLOAT) END) AS avg_delay_seconds
            FROM liveboard_records lr
            INNER JOIN stations s ON s.station_id = lr.station_id
            WHERE s.name IN ({placeholders})
            GROUP BY s.name
            HAVING SUM(CASE WHEN lr.canceled = 0 THEN 1 ELSE 0 END) > 0
            ORDER BY on_time_pct DESC;
        """, MAJOR_HUBS)
        rows = cursor.fetchall()
        cursor.close()

        result = [
            {
                "station_name": row.station_name,
                "total_records": row.total_records,
                "on_time_pct": float(row.on_time_pct) if row.on_time_pct is not None else 0.0,
                "avg_delay_seconds": float(row.avg_delay_seconds) if row.avg_delay_seconds is not None else 0.0,
            }
            for row in rows
        ]
        return _json_response(result)
    except Exception as e:
        correlation_id = _log_failure("Leaderboard stats query", e)
        return _json_response(
            {"error": "query failed", "reference": correlation_id}, status_code=500
        )
    finally:
        if conn is not None:
            conn.close()


@app.route(route="dashboard", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
def dashboard_http(req: func.HttpRequest) -> func.HttpResponse:
    """Serve the dashboard from the same Azure Function App as its API."""
    dashboard_path = os.path.join(os.path.dirname(__file__), "station_stats_dashboard.html")
    try:
        with open(dashboard_path, "r", encoding="utf-8") as dashboard_file:
            return func.HttpResponse(dashboard_file.read(), mimetype="text/html")
    except OSError as e:
        logging.error(f"Dashboard file could not be served: {e}")
        return func.HttpResponse("Dashboard unavailable.", status_code=500)