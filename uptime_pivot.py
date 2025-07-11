"""
CSV Pivot Tool for Pod Uptime Analysis (v1.1.0)

This tool reads a CSV containing pod metadata and generates pivot tables or
flattened summaries based on fields like namespace, pod name, and pod uptime.

Supports two types of uptime representation:
- Bucketed classification (e.g., 0-3 months, >2 years)
- Raw uptime in days (--uptime-as-days)

Example usage:

1. Pivot with bucketed uptime:
   python pivot_tool.py -i pods.csv -o output.csv --rows namespace podname --columns uptime region

2. Pivot with raw uptime in days:
   python pivot_tool.py -i pods.csv -o output.csv --rows namespace --columns uptime --uptime-as-days

3. Flattened format with uptime as a row:
   python pivot_tool.py -i pods.csv -o output.csv --uptime-as-row --rows namespace

4. Print CSV headers (dry run):
   python pivot_tool.py -i pods.csv --dry-run
"""

import argparse
import csv
import logging
from collections import defaultdict
from datetime import datetime

__version__ = "1.1.0"


def classify_pod_age(start_epoch, current_time):
    """Classify pod age in bucketed ranges based on the start time."""
    try:
        start_time = datetime.utcfromtimestamp(float(start_epoch))
    except (ValueError, OverflowError):
        return "invalid"

    delta = current_time - start_time
    if delta.total_seconds() < 0:
        return "invalid"

    days = delta.days
    if days > 730:
        return ">2 years"
    if 365 <= days <= 730:
        return "1-2 years"
    if 180 <= days < 365:
        return "6-12 months"
    if 90 <= days < 180:
        return "3-6 months"
    if 0 <= days < 90:
        return "0-3 months"

    return "invalid"


def days_since_epoch(start_epoch, now):
    """Calculate the number of days since the start_epoch."""
    try:
        start_time = datetime.utcfromtimestamp(float(start_epoch))
    except (ValueError, OverflowError):
        return "invalid"

    delta = now - start_time
    if delta.total_seconds() < 0:
        return "invalid"

    return str(delta.days)


def pivot_data(input_file, output_file, row_fields, column_fields,
               include_invalid, now, uptime_as_days):
    """Pivot the CSV data into a grouped count table."""
    data = defaultdict(lambda: defaultdict(int))
    unique_columns = set()

    with open(input_file, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)

        for row_num, row in enumerate(reader, 1):
            col_vals = []
            skip_row = False

            for col_field in column_fields:
                if col_field == "uptime":
                    start_epoch = row.get("starttime")
                    if not start_epoch:
                        col_val = "invalid" if include_invalid else None
                    else:
                        col_val = (
                            days_since_epoch(start_epoch, now)
                            if uptime_as_days else
                            classify_pod_age(start_epoch, now)
                        )
                        if col_val == "invalid" and not include_invalid:
                            col_val = None
                else:
                    col_val = row.get(col_field)

                if col_val is None:
                    skip_row = True
                    break

                col_vals.append(col_val)

            if skip_row:
                continue

            col_key = tuple(col_vals) if len(col_vals) > 1 else col_vals[0]

            try:
                row_key = tuple(row[field] for field in row_fields)
            except KeyError as e:
                logging.warning(f"Missing field {e} at row {row_num}. Skipping row.")
                continue

            data[row_key][col_key] += 1
            unique_columns.add(col_key)

    unique_columns = sorted(unique_columns)

    def format_col_key(key):
        return "|".join(key) if isinstance(key, tuple) else key

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(list(row_fields) + [format_col_key(c) for c in unique_columns])
        for row_key, counts in sorted(data.items()):
            row = list(row_key)
            for col in unique_columns:
                row.append(counts.get(col, 0))
            writer.writerow(row)


def flatten_data(input_file, output_file, row_fields,
                 include_invalid, now, uptime_as_days):
    """Output a flattened CSV with uptime as a row field."""
    summary = defaultdict(int)

    with open(input_file, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)

        if "starttime" not in reader.fieldnames:
            logging.error("Error: 'starttime' column missing from input CSV.")
            return

        for row_num, row in enumerate(reader, 1):
            try:
                row_key = [row[field] for field in row_fields] if row_fields else []
            except KeyError as e:
                logging.warning(f"Missing field {e} at row {row_num}. Skipping row.")
                continue

            start_epoch = row.get("starttime")
            if not start_epoch:
                uptime = "invalid" if include_invalid else None
            else:
                uptime = (
                    days_since_epoch(start_epoch, now)
                    if uptime_as_days else
                    classify_pod_age(start_epoch, now)
                )
                if uptime == "invalid" and not include_invalid:
                    uptime = None

            if uptime is None:
                continue

            full_key = tuple(row_key + [uptime])
            summary[full_key] += 1

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow((row_fields if row_fields else []) + ["uptime", "count"])
        for key, count in sorted(summary.items()):
            writer.writerow(list(key) + [count])


def main():
    """Main entry point for CLI argument parsing and execution."""
    parser = argparse.ArgumentParser(
        description="Flexible pivot tool for pod uptime or other CSV fields"
    )
    parser.add_argument(
        "-i", "--input", required=True,
        help="Input CSV file path"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output CSV file path (unless --dry-run)"
    )
    parser.add_argument(
        "--rows", nargs="+",
        help="Fields to use as row keys"
    )
    parser.add_argument(
        "--columns", nargs="+",
        help="Fields to pivot as columns (e.g., uptime, region)"
    )
    parser.add_argument(
        "--include-invalid", action="store_true",
        help="Include invalid/future starttimes"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print available CSV fields and exit"
    )
    parser.add_argument(
        "--uptime-as-row", action="store_true",
        help="Use uptime as a row field instead of column"
    )
    parser.add_argument(
        "--uptime-as-days", action="store_true",
        help="Use exact number of days instead of bucketed uptime"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose output including warnings"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s"
    )

    if args.dry_run:
        with open(args.input, "r", encoding="utf-8", newline="") as infile:
            reader = csv.reader(infile)
            headers = next(reader)
            print("Available fields in input CSV:")
            for field in headers:
                print(f"  - {field}")
        return

    if not args.output:
        parser.error("Missing required argument: -o/--output (unless using --dry-run)")

    if not args.rows and not args.uptime_as_row:
        parser.error("Missing required argument: --rows (unless using --uptime-as-row)")

    now = datetime.utcnow()

    if args.uptime_as_row:
        flatten_data(
            input_file=args.input,
            output_file=args.output,
            row_fields=args.rows,
            include_invalid=args.include_invalid,
            now=now,
            uptime_as_days=args.uptime_as_days,
        )
    else:
        if not args.columns:
            parser.error("Missing required argument: --columns (unless using --uptime-as-row)")
        pivot_data(
            input_file=args.input,
            output_file=args.output,
            row_fields=args.rows,
            column_fields=args.columns,
            include_invalid=args.include_invalid,
            now=now,
            uptime_as_days=args.uptime_as_days,
        )

    logging.info(f"Output saved to '{args.output}'")


if __name__ == "__main__":
    main()
