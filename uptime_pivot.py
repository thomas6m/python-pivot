"""
Flexible CSV pivot tool focused on pod uptime classification.

Reads a CSV file, groups rows by specified row fields, and pivots data on one or more
column fields, counting occurrences per group. Supports special "uptime" pivot
column to bucket pods by their age based on a starttime epoch.

Usage examples:

Default pivot (uptime in columns):
    python pivot_tool.py -i pods.csv -o pivot.csv --rows namespace podname --columns uptime region

Uptime as row field:
    python pivot_tool.py -i pods.csv -o flat.csv --rows namespace podname --uptime-as-row

Uptime count only (no grouping rows):
    python pivot_tool.py -i pods.csv -o counts.csv --uptime-as-row

Author: Your Name
"""

import argparse
import csv
import logging
from collections import defaultdict
from datetime import datetime

__version__ = "1.0.1"


def classify_pod_age(start_epoch, current_time):
    """
    Classify pod uptime into defined buckets based on a starttime epoch.

    Handles invalid input and future timestamps (treated as 'invalid').

    Args:
        start_epoch (str or float): Epoch timestamp string or float representing
            the pod start time.
        current_time (datetime): Current datetime to compare against.

    Returns:
        str: Bucket name representing pod uptime category, or 'invalid' if the
            input is invalid or represents a future start time.
    """
    try:
        start_time = datetime.utcfromtimestamp(float(start_epoch))
    except (ValueError, OverflowError):
        return "invalid"

    delta = current_time - start_time

    # Future start times are invalid
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


def pivot_data(input_file, output_file, row_fields, column_fields,
               include_invalid, now):
    """
    Pivot data with uptime or other fields as columns, counting occurrences per group.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to the output CSV file.
        row_fields (list[str]): List of fields to group rows by.
        column_fields (list[str]): List of fields to pivot as columns.
        include_invalid (bool): Whether to include invalid/future starttimes in uptime.
        now (datetime): Current datetime for uptime classification.

    Writes:
        A CSV file with rows grouped by `row_fields` and columns based on unique
        combinations of `column_fields`, containing counts of occurrences.
    """
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
                        col_val = classify_pod_age(start_epoch, now)
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
        """Format the column key for CSV header output."""
        return "|".join(key) if isinstance(key, tuple) else key

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(list(row_fields) + [format_col_key(c) for c in unique_columns])
        for row_key, counts in sorted(data.items()):
            row = list(row_key)
            for col in unique_columns:
                row.append(counts.get(col, 0))
            writer.writerow(row)


def flatten_data(input_file, output_file, row_fields, include_invalid, now):
    """
    Output a flattened CSV with uptime as a row field, aggregating counts.

    Supports counting uptime alone if row_fields is empty.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to the output CSV file.
        row_fields (list[str] or None): List of fields to use as row keys.
        include_invalid (bool): Whether to include invalid/future starttimes in uptime.
        now (datetime): Current datetime for uptime classification.

    Writes:
        A CSV file with rows containing the combination of row fields and uptime
        bucket, along with counts of occurrences.
    """
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
                uptime = classify_pod_age(start_epoch, now)
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
    """
    Parse command-line arguments and execute pivot or flatten operation.

    Usage:
        Use --uptime-as-row flag to flatten uptime as rows.
        Otherwise, pivot uptime or other fields as columns.

    Raises:
        SystemExit: If required arguments are missing or invalid.
    """
    parser = argparse.ArgumentParser(
        description="Flexible pivot tool for pod uptime or other CSV fields"
    )
    parser.add_argument(
        "-i", "--input", required=True, help="Input CSV file path"
    )
    parser.add_argument(
        "-o", "--output", required=False,
        help="Output CSV file path (unless --dry-run)"
    )
    parser.add_argument(
        "--rows", nargs="+", required=False,
        help="Fields to use as row keys (required unless --uptime-as-row with no grouping)"
    )
    parser.add_argument(
        "--columns", nargs="+", required=False,
        help="Fields to pivot as columns (e.g., uptime, region)"
    )
    parser.add_argument(
        "--include-invalid", action="store_true",
        help="Include invalid/future starttimes in uptime"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print available CSV fields and exit"
    )
    parser.add_argument(
        "--uptime-as-row", action="store_true",
        help="Use uptime as a row field instead of column"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose output including warnings"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
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
        )
    else:
        if not args.columns:
            parser.error(
                "Missing required argument: --columns (unless using --uptime-as-row)"
            )
        pivot_data(
            input_file=args.input,
            output_file=args.output,
            row_fields=args.rows,
            column_fields=args.columns,
            include_invalid=args.include_invalid,
            now=now,
        )

    logging.info(f"Output saved to '{args.output}'")


if __name__ == "__main__":
    main()
