# Hierarchical Data Transformer

[![PyPI version](https://badge.fury.io/py/hierarchical-transformer.svg)](https://badge.fury.io/py/hierarchical-transformer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

A Python library for transforming hierarchical spreadsheet data into normalized tabular format. This library specializes in converting complex, multi-row spreadsheet data into clean, analysis-ready tabular formats.

## Example Transformation

### Before (Original Hierarchical Format)
![Admission Census Before](example_hierarchical.png)

### After (Transformed Tabular Format)
![Admission Census Before](example_tabular.png)

## Features

- Transform hierarchical data into flat, tabular structure
- Configurable field mapping and aliases
- Robust data validation
- Flexible search patterns for value extraction
- Support for custom data processing rules
- Comprehensive logging and error reporting

## Installation

```bash
pip install hierarchical-transformer
```

## Quick Start

```python
from hierarchical_transformer import DataTransformer, TransformerConfig

# Define configuration
config = TransformerConfig(
    identifier_field="Patient Name",
    target_fields=[
        "Patient Name",
        "PAN",
        "Admit Date",
        "Current Cert Period",
        "Primary Diagnosis",
        "Case Manager / Primary RN",
        "Emergency Contact"
    ],
    date_columns=["Admit Date"]
)

# Initialize transformer
transformer = DataTransformer()

# Transform data
result_df = transformer.transform(input_df, config)
```

## Configuration Options

The `TransformerConfig` class supports the following parameters:

| Parameter | Type | Description | Default |
|-----------|------|-------------|----------|
| skip_rows | int | Number of header rows to skip | 0 |
| drop_columns | List[int] | Column indices to drop | None |
| date_columns | List[str] | Columns to convert to datetime | None |
| identifier_field | str | Field that marks start of new record | Required |
| target_fields | List[str] | Fields to extract | Required |
| field_aliases | Dict[str, str] | Alternative names for fields | None |
| search_radius | int | Rows to search for values | 10 |
| column_search_radius | int | Columns to search right | 5 |

## Data Validation

```python
from hierarchical_transformer import DataValidator

# Create validator
validator = DataValidator()

# Define validation rules
rules = {
    "required_columns": ["Patient Name", "PAN", "Admit Date"],
    "date_format": {
        "Admit Date": "%m/%d/%Y"
    },
    "unique_columns": ["PAN"]
}

# Validate data
result = validator.validate(df, rules)
if result.is_valid:
    transformed_df = transformer.transform(df, config)
else:
    print("Validation errors:", result.errors)
```


## Use Cases

### Financial Data Processing

```python
financial_config = TransformerConfig(
    identifier_field="Account Number",
    target_fields=[
        "Account Number",
        "Transaction Date",
        "Description",
        "Debit Amount",
        "Credit Amount",
        "Balance"
    ],
    date_columns=["Transaction Date"],
    field_aliases={
        "Acct #": "Account Number",
        "Trans Date": "Transaction Date"
    }
)
```

### Inventory Management

```python
inventory_config = TransformerConfig(
    identifier_field="SKU",
    target_fields=[
        "SKU",
        "Product Name",
        "Category",
        "Quantity",
        "Location",
        "Last Updated"
    ],
    date_columns=["Last Updated"],
    search_radius=15
)
```

### Customer Orders

```python
order_config = TransformerConfig(
    skip_rows=2,
    identifier_field="Order ID",
    target_fields=[
        "Order ID",
        "Customer Name",
        "Shipping Address",
        "Order Date",
        "Total Amount"
    ],
    date_columns=["Order Date"]
)
```

## Advanced Usage

### Custom Value Extraction

```python
class CustomTransformer(DataTransformer):
    def _extract_field_value(self, df, row_idx, field_aliases, config):
        value = super()._extract_field_value(df, row_idx, field_aliases, config)
        if value:
            # Apply custom processing
            return self._custom_process_value(value)
        return value
```

### Custom Validation Rules

```python
def validate_order_amounts(df):
    """Custom validation for order amounts."""
    invalid_orders = df[df['Total'] != df['Items'].sum()]
    return {
        'valid': len(invalid_orders) == 0,
        'message': f"Mismatched order totals in rows: {invalid_orders.index.tolist()}"
    }

validation_rules = {
    "custom_validations": [{
        "function": validate_order_amounts
    }]
}
```

### Logging Configuration

```python
import logging

transformer = DataTransformer(
    logger=logging.getLogger("custom_logger")
)
```

## Common Patterns

### Handling Missing Data

```python
config = TransformerConfig(
    identifier_field="ID",
    target_fields=fields,
    missing_value_handling="skip"  # or "fill" or "error"
)
```

### Multi-line Field Processing

```python
config = TransformerConfig(
    identifier_field="Record ID",
    target_fields=fields,
    search_radius=20,  # Increased search radius for multi-line fields
    column_search_radius=8  # Wider search for scattered values
)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone the repository
git clone https://github.com/username/hierarchical-transformer.git

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

### [1.0.0] - 2024-12-10
- Initial release
- Basic transformation functionality
- Data validation features
- Documentation and examples

## Support

For support, please open an issue in the GitHub repository or contact the maintainers.
