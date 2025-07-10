# CSV Pivot Tool Runbook

## Overview

The CSV Pivot Tool is a Python utility designed to analyze pod uptime data by pivoting CSV files. It can group data by specified fields and classify pods based on their age using epoch timestamps. The tool supports both traditional pivoting (columns) and flattened output (rows).

## Features

- **Pod Age Classification**: Automatically categorizes pods into age buckets (0-3 months, 3-6 months, etc.)
- **Flexible Pivoting**: Supports both column-based and row-based pivoting
- **Data Validation**: Handles invalid timestamps and missing data gracefully
- **Multiple Output Formats**: Choose between pivot tables or flattened summaries

## Requirements

- Python 3.6+
- Standard library modules: `argparse`, `csv`, `logging`, `collections`, `datetime`

## Installation

No special installation required. Simply save the script as `pivot_tool.py` and ensure Python is available in your environment.

## Input Data Format

The tool expects a CSV file with the following structure:

```csv
namespace,podname,starttime,region
default,web-app-1,1640995200,us-east-1
kube-system,dns-pod,1609459200,us-west-2
```

### Required Fields
- **starttime**: Epoch timestamp (seconds since Unix epoch)
- Any additional fields you want to group by or pivot on

### Optional Fields
- **namespace**: Pod namespace
- **podname**: Pod name
- **region**: Deployment region
- Any other categorical fields

## Usage Examples

### 1. Basic Pivot (Default Mode)
Groups pods by namespace and pod name, with uptime buckets as columns:

```bash
python pivot_tool.py -i pods.csv -o pivot.csv --rows namespace podname --columns uptime
```

**Output Structure:**
```csv
namespace,podname,0-3 months,3-6 months,6-12 months,1-2 years,>2 years
default,web-app-1,1,0,0,0,0
kube-system,dns-pod,0,0,0,1,0
```

### 2. Multi-Column Pivot
Pivot on both uptime and region:

```bash
python pivot_tool.py -i pods.csv -o pivot.csv --rows namespace podname --columns uptime region
```

**Output Structure:**
```csv
namespace,podname,0-3 months|us-east-1,0-3 months|us-west-2,3-6 months|us-east-1
default,web-app-1,1,0,0
kube-system,dns-pod,0,1,0
```

### 3. Flattened Output (Uptime as Row)
Use uptime as a row field instead of column:

```bash
python pivot_tool.py -i pods.csv -o flat.csv --rows namespace podname --uptime-as-row
```

**Output Structure:**
```csv
namespace,podname,uptime,count
default,web-app-1,0-3 months,1
kube-system,dns-pod,1-2 years,1
```

### 4. Uptime Count Only
Generate simple uptime distribution without grouping:

```bash
python pivot_tool.py -i pods.csv -o counts.csv --uptime-as-row
```

**Output Structure:**
```csv
uptime,count
0-3 months,5
3-6 months,12
6-12 months,8
1-2 years,3
>2 years,1
```

### 5. Include Invalid Data
Include pods with invalid or future timestamps:

```bash
python pivot_tool.py -i pods.csv -o pivot.csv --rows namespace --columns uptime --include-invalid
```

### 6. Dry Run (Preview Available Fields)
Check what fields are available in your CSV:

```bash
python pivot_tool.py -i pods.csv --dry-run
```

## Command Line Arguments

### Required Arguments
- `-i, --input`: Input CSV file path

### Optional Arguments
- `-o, --output`: Output CSV file path (required unless using --dry-run)
- `--rows`: Fields to use as row keys (space-separated)
- `--columns`: Fields to pivot as columns (space-separated)
- `--uptime-as-row`: Use uptime as a row field instead of column
- `--include-invalid`: Include invalid/future timestamps in results
- `--dry-run`: Display available CSV fields and exit
- `-v, --verbose`: Enable verbose output and warnings

## Pod Age Classification

The tool automatically classifies pods into the following age buckets based on their starttime:

| Bucket | Age Range |
|--------|-----------|
| 0-3 months | 0-89 days |
| 3-6 months | 90-179 days |
| 6-12 months | 180-364 days |
| 1-2 years | 365-730 days |
| >2 years | 731+ days |
| invalid | Invalid timestamp or future date |

## Error Handling

### Common Issues and Solutions

1. **Missing Required Fields**
   - Error: `Missing field 'fieldname' at row X`
   - Solution: Ensure all specified row/column fields exist in your CSV

2. **Invalid Timestamps**
   - Behavior: Automatically classified as "invalid" bucket
   - Solution: Use `--include-invalid` flag to include these records

3. **Missing starttime Column**
   - Error: `'starttime' column missing from input CSV`
   - Solution: Ensure your CSV has a 'starttime' column when using uptime classification

4. **Future Timestamps**
   - Behavior: Classified as "invalid" (likely data entry error)
   - Solution: Review your data for timestamp accuracy

## Performance Considerations

- **Memory Usage**: The tool loads all data into memory for processing
- **Large Files**: For very large CSV files (>1M rows), consider:
  - Splitting input files
  - Using more specific row groupings to reduce output size
  - Monitoring memory usage during processing

## Troubleshooting

### Debug Mode
Enable verbose logging to see detailed processing information:

```bash
python pivot_tool.py -i pods.csv -o output.csv --rows namespace --columns uptime -v
```

### Common Validation Steps

1. **Check CSV Format**
   ```bash
   head -n 5 pods.csv  # View first 5 rows
   ```

2. **Verify Field Names**
   ```bash
   python pivot_tool.py -i pods.csv --dry-run
   ```

3. **Test with Small Sample**
   ```bash
   head -n 100 pods.csv > sample.csv
   python pivot_tool.py -i sample.csv -o test.csv --rows namespace --columns uptime
   ```

## Output File Analysis

### Interpreting Results

- **Pivot Mode**: Each row represents a unique combination of row fields, with counts distributed across column combinations
- **Flatten Mode**: Each row shows the count for a specific combination of row fields and uptime bucket
- **Zero Values**: Indicate no pods found for that combination

### Post-Processing Tips

- Import results into Excel or data analysis tools for further visualization
- Use CSV tools like `csvkit` for additional analysis
- Consider aggregating results for executive summaries

## Version History

- **v1.0.1**: Current version with improved error handling and documentation
- Supports flexible pivoting on multiple fields
- Enhanced uptime classification with invalid data handling

## Support and Maintenance

### Regular Maintenance Tasks

1. **Data Validation**: Regularly check for invalid timestamps in source data
2. **Bucket Adjustment**: Review age classification buckets based on operational needs
3. **Performance Monitoring**: Monitor processing time for large datasets

### Extending the Tool

The tool is designed to be extensible. Common modifications include:

- Adding new age classification buckets
- Supporting additional timestamp formats
- Adding new pivot field types
- Implementing data filtering options

## Security Considerations

- **Input Validation**: The tool validates CSV structure and handles malformed data gracefully
- **File Permissions**: Ensure appropriate read/write permissions for input/output files
- **Data Sensitivity**: Be aware that pod data may contain sensitive information about your infrastructure

## Best Practices

1. **Backup Data**: Always keep backups of original CSV files before processing
2. **Test First**: Use `--dry-run` to validate field names before processing
3. **Start Small**: Test with sample data before processing large files
4. **Document Results**: Keep track of the parameters used for each analysis
5. **Regular Updates**: Update the tool as your data structure evolves
