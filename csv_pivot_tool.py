"""
CSV Pivot Tool with DuckDB (v2.3.1)

This tool reads a CSV file and generates a pivot table based on specified
row and optional column fields. It counts the frequency of unique 
combinations of values across rows and columns using DuckDB SQL engine for 
performance.

You can specify:
- --columns to pivot on column fields
- --no-columns to disable column pivoting and group only by rows

Example usage:

    python pivot_tool.py -i data.csv -o output.csv --rows fieldA fieldB --columns fieldX fieldY
    python pivot_tool.py -i data.csv -o output.csv --rows fieldA fieldB --no-columns
    python pivot_tool.py -i data.csv --dry-run

Author: Your Name
License: MIT
"""

import argparse
import csv
import logging
import sys

import duckdb  # pip install duckdb

__version__ = "2.3.1"


def quote_identifier(name):
    """
    Quote SQL identifiers safely by wrapping in double quotes and escaping
    internal double quotes.

    Args:
        name (str): The identifier name to quote.

    Returns:
        str: Safely quoted identifier.
    """
    return '"' + name.replace('"', '""') + '"'


def escape_literal(value):
    """
    Escape single quotes in SQL string literals to prevent syntax errors.

    Args:
        value (str): The string literal to escape.

    Returns:
        str: Escaped string literal.
    """
    return value.replace("'", "''")


def pivot_data_with_duckdb(input_file, output_file, row_fields, column_fields):
    """
    Use DuckDB to create a pivot table grouped by specified row and column
    fields from a CSV file.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path where the output CSV will be saved.
        row_fields (list of str): Fields to use as row keys.
        column_fields (list of str or None): Fields to pivot as columns, or
            None to disable column pivoting.

    Raises:
        SystemExit: If database or file operations fail.
    """
    con = None
    try:
        # Connect to an in-memory DuckDB instance
        con = duckdb.connect()

        # Create a DuckDB view for the CSV data
        con.execute(f"CREATE VIEW data AS SELECT * FROM read_csv_auto('{input_file}')")

        # Quote row fields for SQL safety
        quoted_row_fields = [quote_identifier(f) for f in row_fields]

        if column_fields:
            # Quote column fields for SQL safety
            quoted_column_fields = [quote_identifier(f) for f in column_fields]

            # Concatenate column fields for pivot key using safe SQL concatenation
            if len(quoted_column_fields) > 1:
                col_concat = " || '|' || ".join(quoted_column_fields)
            else:
                col_concat = quoted_column_fields[0]

            # Build WHERE clauses to exclude NULLs safely
            col_not_null = [f"{f} IS NOT NULL" for f in quoted_column_fields]
            row_not_null = [f"{f} IS NOT NULL" for f in quoted_row_fields]

            # Query distinct pivot column values
            distinct_cols_query = f"""
                SELECT DISTINCT {col_concat} AS col_key
                FROM data
                WHERE {" AND ".join(col_not_null)}
                ORDER BY col_key
            """
            distinct_cols = [row[0] for row in con.execute(distinct_cols_query).fetchall()]

            if not distinct_cols:
                logging.warning("No distinct columns found to pivot on.")
                return

            # Build CASE statements for pivot aggregation
            case_statements = []
            for col_val in distinct_cols:
                safe_val = escape_literal(col_val)
                case = (
                    f"SUM(CASE WHEN ({col_concat}) = '{safe_val}' THEN 1 ELSE 0 END) "
                    f"AS {quote_identifier(col_val)}"
                )
                case_statements.append(case)

            pivot_query = f"""
                SELECT
                    {', '.join(quoted_row_fields)},
                    {', '.join(case_statements)}
                FROM data
                WHERE {" AND ".join(row_not_null)}
                GROUP BY {', '.join(quoted_row_fields)}
                ORDER BY {', '.join(quoted_row_fields)}
            """

            result = con.execute(pivot_query).fetchall()
            columns = [desc[0] for desc in con.description]

        else:
            # Group by row fields and count occurrences when no columns pivot
            row_not_null = [f"{f} IS NOT NULL" for f in quoted_row_fields]

            pivot_query = f"""
                SELECT
                    {', '.join(quoted_row_fields)},
                    COUNT(*) AS count
                FROM data
                WHERE {" AND ".join(row_not_null)}
                GROUP BY {', '.join(quoted_row_fields)}
                ORDER BY {', '.join(quoted_row_fields)}
            """

            result = con.execute(pivot_query).fetchall()
            columns = [desc[0] for desc in con.description]

        # Write results to output CSV file
        try:
            with open(output_file, "w", encoding="utf-8", newline="") as outfile:
                writer = csv.writer(outfile)
                writer.writerow(columns)
                writer.writerows(result)
        except IOError as e:
            logging.error(f"Error writing output file: {e}")
            sys.exit(1)

    except (duckdb.Error, Exception) as e:
        logging.error(f"Database query error: {e}")
        sys.exit(1)

    finally:
        if con:
            con.close()


def main():
    """
    Main CLI interface.

    Parses arguments, handles dry-run mode to print CSV headers, and
    calls the pivot function with appropriate parameters.
    """
    parser = argparse.ArgumentParser(
        description=(
            "CSV pivot tool using DuckDB for grouping and counting "
            "field combinations."
        )
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Input CSV file path",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output CSV file path (unless using --dry-run)",
    )
    parser.add_argument(
        "--rows",
        nargs="+",
        help="Fields to use as row keys",
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        help="Fields to pivot as columns (e.g., region, status)",
    )
    parser.add_argument(
        "--no-columns",
        action="store_true",
        help="Do not pivot by columns, group only by rows",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print available CSV headers and exit",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    if args.dry_run:
        try:
            with open(args.input, "r", encoding="utf-8", newline="") as infile:
                reader = csv.reader(infile)
                headers = next(reader)
                print("\nAvailable fields in input CSV:\n")
                for field in headers:
                    print(f"  - {field}")
                print()
        except IOError as e:
            logging.error(f"Error reading input file: {e}")
            sys.exit(1)
        return

    if not args.output:
        parser.error("Missing required argument: -o/--output")

    if not args.rows:
        parser.error("Missing required argument: --rows")

    if args.columns and args.no_columns:
        parser.error("Cannot specify both --columns and --no-columns")

    column_fields = None if args.no_columns else args.columns

    pivot_data_with_duckdb(
        input_file=args.input,
        output_file=args.output,
        row_fields=args.rows,
        column_fields=column_fields,
    )

    logging.info(f"Output saved to '{args.output}'")


if __name__ == "__main__":
    main()
