"""
Flexible CSV pivot tool focused on pod uptime classification.

Reads a CSV file, groups rows by specified row fields, and pivots data on one or more
column fields, counting occurrences per group. Supports special "uptime" pivot
column to bucket pods by their age based on a starttime epoch.

Usage example:
    python pivot_tool.py -i pods.csv -o pivot.csv --rows namespace podname --columns uptime region

Author: Your Name
"""

import csv
import argparse
from datetime import datetime
from collections import defaultdict


def classify_pod_age(start_epoch, current_time):
    """
    Classify pod uptime into defined buckets based on starttime epoch.

    Args:
        start_epoch (str or float): Pod start time in epoch seconds.
        current_time (datetime): Current UTC datetime.

    Returns:
        str: Uptime bucket label or "invalid" if timestamp invalid.
    """
    try:
        start_time = datetime.utcfromtimestamp(float(start_epoch))
    except (ValueError, OverflowError):
        return "invalid"

    delta = current_time - start_time
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


def pivot_data(input_file, output_file, row_fields, column_fields, include_invalid):
    """
    Reads input CSV and pivots data based on given row fields and one or more column fields.
    If any column_field == 'uptime', calculates uptime buckets based on 'starttime' epoch.
    Counts occurrences and writes pivot table to output CSV.

    Args:
        input_file (str): Path to input CSV file.
        output_file (str): Path to output CSV file.
        row_fields (list[str]): List of field names to use as row keys.
        column_fields (list[str]): List of field names to pivot as columns ('uptime' supported).
        include_invalid (bool): Whether to include invalid/future starttime pods.
    """
    now = datetime.utcnow()
    data = defaultdict(lambda: defaultdict(int))
    unique_columns = set()

    with open(input_file, "r", encoding="utf-8") as infile:
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
                        # Explicitly None, skip row
                        col_val = None

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
                print(f"Warning: Missing field {e} at row {row_num}. Skipping row.")
                continue

            data[row_key][col_key] += 1
            unique_columns.add(col_key)

    unique_columns = sorted(unique_columns)

    def format_col_key(key):
        """Format column key tuple into a string with '|' delimiter."""
        if isinstance(key, tuple):
            return "|".join(key)
        return key

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        header = list(row_fields) + [format_col_key(c) for c in unique_columns]
        writer.writerow(header)

        for row_key, counts in sorted(data.items()):
            row = list(row_key)
            for col in unique_columns:
                row.append(counts.get(col, 0))
            writer.writerow(row)


def main():
    """
    Parse CLI arguments and run the pivot operation or dry-run to show fields.
    """
    parser = argparse.ArgumentParser(
        description="Flexible pivot tool for pod uptime or other CSV fields"
    )
    parser.add_argument(
        "-i", "--input", required=True, help="Input CSV file path"
    )
    parser.add_argument(
        "-o", "--output", required=False,
        help="Output CSV file path (required unless --dry-run)"
    )
    parser.add_argument(
        "--rows", nargs="+", required=False,
        help="Fields to use as row keys (space-separated)"
    )
    parser.add_argument(
        "--columns", nargs="+", required=False,
        help="Field(s) to pivot as columns (use 'uptime' for uptime buckets, multiple allowed)"
    )
    parser.add_argument(
        "--include-invalid", action="store_true",
        help="Include invalid/future starttimes in uptime"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print available CSV fields and exit"
    )

    args = parser.parse_args()

    if args.dry_run:
        with open(args.input, "r", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            headers = next(reader)
            print("Available fields in input CSV:")
            for field in headers:
                print(f"  - {field}")
        return

    if not args.output:
        parser.error("the following arguments are required: -o/--output (unless --dry-run)")

    if not args.rows or not args.columns:
        parser.error("the following arguments are required: --rows and --columns (unless --dry-run)")

    pivot_data(
        input_file=args.input,
        output_file=args.output,
        row_fields=args.rows,
        column_fields=args.columns,
        include_invalid=args.include_invalid,
    )

    print(f"Pivot complete. Output saved to '{args.output}'")


if __name__ == "__main__":
    main()
