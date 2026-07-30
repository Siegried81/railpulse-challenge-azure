import argparse
import datetime as dt
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyodbc


BASE_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = BASE_DIR / "docs"
DEFAULT_OUTPUT_PATH = DOCS_DIR / "network_leaderboard.png"
DEFAULT_ON_TIME_THRESHOLD_SECONDS = 60
DEFAULT_LIMIT = 10
ALLOWED_SORT_BY = {"on-time", "avg-delay"}

DEFAULT_STATIONS = [
    "Bruxelles-Central",
    "Anvers-Central",
    "Gand-Saint-Pierre",
    "Charleroi-Central",
    "Liège-Guillemins",
]


def get_db_connection() -> pyodbc.Connection:
    connection_string = os.environ.get("SQL_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError(
            "SQL_CONNECTION_STRING is not set. "
            "Set it in the Function App Application Settings or in your local shell before running the script."
        )
    return pyodbc.connect(connection_string)


def fetch_leaderboard(
    connection: pyodbc.Connection,
    on_time_threshold_seconds: int,
    limit: int,
    pulled_on: dt.date | None,
    sort_by: str,
    station_names: list[str] | None,
):
    """
    station_names restricts the result set to exactly those stations
    (matched by s.name) instead of a dynamic TOP(limit) ranking.
    """
    cursor = connection.cursor()
    where_clauses = []
    query_params: list[object] = [on_time_threshold_seconds]

    if pulled_on is not None:
        where_clauses.append("CAST(lr.pulled_at AS date) = ?")
        query_params.append(pulled_on)

    if station_names:
        placeholders = ", ".join("?" for _ in station_names)
        where_clauses.append(f"s.name IN ({placeholders})")
        query_params.extend(station_names)

    where_sql = ""
    if where_clauses:
        where_sql = " AND " + " AND ".join(where_clauses)

    order_by_clause = (
        "ORDER BY on_time_pct DESC, avg_delay_seconds ASC, total_records DESC, station_name ASC"
        if sort_by == "on-time"
        else "ORDER BY avg_delay_seconds DESC, on_time_pct ASC, total_records DESC, station_name ASC"
    )

    top_clause = "" if station_names else f"TOP ({limit})"

    cursor.execute(
        f"""
        SELECT {top_clause}
            s.name AS station_name,
            COUNT(*) AS total_records,
            SUM(CASE WHEN lr.canceled = 0 THEN 1 ELSE 0 END) AS active_records,
            CAST(ROUND(AVG(CASE WHEN lr.canceled = 0 THEN CAST(lr.delay_seconds AS FLOAT) END), 1) AS FLOAT) AS avg_delay_seconds,
            ROUND(
                SUM(CASE WHEN lr.canceled = 0 AND lr.delay_seconds <= ? THEN 1 ELSE 0 END) * 100.0
                / NULLIF(SUM(CASE WHEN lr.canceled = 0 THEN 1 ELSE 0 END), 0),
                2
            ) AS on_time_pct
        FROM liveboard_records lr
        INNER JOIN stations s ON s.station_id = lr.station_id
        WHERE 1 = 1{where_sql}
        GROUP BY s.name
        HAVING SUM(CASE WHEN lr.canceled = 0 THEN 1 ELSE 0 END) > 0
        {order_by_clause};
        """,
        query_params,
    )
    return cursor.fetchall()


def print_leaderboard(rows, on_time_threshold_seconds: int, pulled_on: dt.date | None, sort_by: str):
    header = f"RailPulse Network Leaderboard (on-time within {on_time_threshold_seconds}s, sorted by {sort_by})"
    if pulled_on is not None:
        header += f" for {pulled_on.isoformat()}"
    print(f"\n{header}")
    print(f"{'Station':<25} | {'Records':>8} | {'Active':>8} | {'Avg delay (s)':>13} | {'On-time %':>10}")
    print("-" * 78)
    for station_name, total_records, active_records, avg_delay_seconds, on_time_pct in rows:
        avg_delay_value = None if avg_delay_seconds is None else float(avg_delay_seconds)
        on_time_value = None if on_time_pct is None else float(on_time_pct)
        avg_delay_display = "n/a" if avg_delay_value is None else f"{avg_delay_value:.1f}"
        on_time_display = "n/a" if on_time_value is None else f"{on_time_value:.2f}%"
        print(
            f"{station_name:<25} | {total_records:>8} | {active_records:>8} | "
            f"{avg_delay_display:>13} | {on_time_display:>10}"
        )


def save_chart(rows, on_time_threshold_seconds: int, output_path: Path, pulled_on: dt.date | None, sort_by: str) -> None:
    stations = [row.station_name for row in rows]
    avg_delay_seconds = [None if row.avg_delay_seconds is None else float(row.avg_delay_seconds) for row in rows]
    on_time_pcts = [None if row.on_time_pct is None else float(row.on_time_pct) for row in rows]

    if sort_by == "avg-delay":
        plotted_values = [0 if value is None else value for value in avg_delay_seconds]
        secondary_values = on_time_pcts
        value_label = "Avg delay (s)"
        bar_color = "#F59E0B"
        axis_limit = max(60, max(plotted_values, default=0) + 60)
    else:
        plotted_values = [0 if value is None else value for value in on_time_pcts]
        secondary_values = avg_delay_seconds
        value_label = "On-time departures (%)"
        bar_color = "#38BDF8"
        axis_limit = 100

    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 7), facecolor="#0f172a")
    ax.set_facecolor("#1e293b")

    bars = ax.barh(stations, plotted_values, color=bar_color, edgecolor="#e2e8f0", linewidth=0.35)
    ax.set_xlabel(value_label, color="#e2e8f0")

    chart_title = f"RailPulse Network Leaderboard — {value_label.lower()}"
    if pulled_on is not None:
        chart_title += f" on {pulled_on.isoformat()}"
    chart_title += f" ({sort_by})"
    ax.set_title(chart_title, color="#f8fafc", pad=18)

    subtitle = f"Top {len(rows)} stations • threshold {on_time_threshold_seconds}s"
    if pulled_on is not None:
        subtitle += f" • snapshot {pulled_on.isoformat()}"
    ax.text(0.0, 1.03, subtitle, transform=ax.transAxes, color="#cbd5e1", fontsize=10)

    ax.set_xlim(0, axis_limit)
    ax.grid(axis="x", color="#334155", alpha=0.7, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(axis="x", colors="#cbd5e1")
    ax.tick_params(axis="y", colors="#f8fafc")
    for spine in ax.spines.values():
        spine.set_color("#475569")

    ax.invert_yaxis()

    for bar, primary_value, secondary_value in zip(bars, plotted_values, secondary_values):
        primary_text = "n/a" if primary_value is None else f"{primary_value:.2f}" if sort_by == "on-time" else f"{primary_value:.1f}"
        secondary_text = "n/a"
        if secondary_value is not None:
            secondary_text = f"{secondary_value:.1f}s" if sort_by == "on-time" else f"{secondary_value:.2f}%"

        ax.text(
            bar.get_width() + (axis_limit * 0.01),
            bar.get_y() + bar.get_height() / 2,
            f"{primary_text} | {secondary_text}",
            va="center",
            ha="left",
            color="#f8fafc",
            fontsize=9,
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)


def parse_stations_arg(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    if raw.strip().lower() in ("", "all"):
        return []
    return [name.strip() for name in raw.split(",") if name.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a network leaderboard from Azure SQL liveboard data.")
    parser.add_argument("--threshold", type=int, default=DEFAULT_ON_TIME_THRESHOLD_SECONDS,
                         help="Maximum delay in seconds counted as on-time.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                         help="Number of stations to display when --stations all is used. Default: 10.")
    parser.add_argument("--sort-by", choices=sorted(ALLOWED_SORT_BY), default="on-time",
                         help="Ranking metric to use for the leaderboard.")
    parser.add_argument("--date", type=str, default=None,
                         help="Optional pulled_at date filter in YYYY-MM-DD format (UTC).")
    parser.add_argument("--stations", type=str, default=None,
                         help=("Comma-separated exact station names to include. Defaults to the "
                               f"fixed hub list baked into the script ({', '.join(DEFAULT_STATIONS)}). "
                               'Pass --stations all to fall back to the old TOP(limit) ranking instead.'))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH,
                         help="Path where the chart image will be saved.")
    args = parser.parse_args()

    pulled_on = None
    if args.date is not None:
        try:
            pulled_on = dt.date.fromisoformat(args.date)
        except ValueError as exc:
            raise SystemExit("--date must use YYYY-MM-DD format") from exc

    parsed = parse_stations_arg(args.stations)
    station_names = DEFAULT_STATIONS if parsed is None else (parsed or None)

    connection = get_db_connection()
    try:
        rows = fetch_leaderboard(
            connection, args.threshold, args.limit, pulled_on, args.sort_by, station_names
        )
    finally:
        connection.close()

    if not rows:
        print(
            "No data in liveboard_records yet. Run the Function App ingestion first, then re-run this script."
        )
        return

    print_leaderboard(rows, args.threshold, pulled_on, args.sort_by)
    save_chart(rows, args.threshold, args.output, pulled_on, args.sort_by)
    print(f"\nChart saved to {args.output}")


if __name__ == "__main__":
    main()