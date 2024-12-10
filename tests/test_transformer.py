# tests/test_transformer.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from hierarchical_transformer import DataTransformer, TransformerConfig, DataValidator

@pytest.fixture
def sample_data():
    """Create sample hierarchical data for testing."""
    data = [
        ["", "", "", "", ""],
        ["", "", "", "", ""],
        ["Customer ID", "12345", "", "", ""],
        ["Name", "John Doe", "", "", ""],
        ["Address", "123 Main St", "", "", ""],
        ["Order Date", "2024-01-15", "", "", ""],
        ["Total", "500.00", "", "", ""],
        ["", "", "", "", ""],
        ["Customer ID", "12346", "", "", ""],
        ["Name", "Jane Smith", "", "", ""],
        ["Address", "456 Oak Ave", "", "", ""],
        ["Order Date", "2024-01-16", "", "", ""],
        ["Total", "750.50", "", "", ""],
    ]
    return pd.DataFrame(data)

@pytest.fixture
def basic_config():
    """Create basic transformer configuration."""
    return TransformerConfig(
        skip_rows=2,
        identifier_field="Customer ID",
        target_fields=[
            "Customer ID",
            "Name",
            "Address",
            "Order Date",
            "Total"
        ],
        date_columns=["Order Date"]
    )

class TestDataTransformer:
    def test_basic_transformation(self, sample_data, basic_config):
        """Test basic data transformation functionality."""
        transformer = DataTransformer()
        result = transformer.transform(sample_data, basic_config)
        
        assert len(result) == 2
        assert list(result.columns) == ['customer_id', 'name', 'address', 'order_date', 'total']
        assert result.iloc[0]['customer_id'] == '12345'
        assert result.iloc[1]['name'] == 'Jane Smith'

    def test_empty_dataframe(self, basic_config):
        """Test handling of empty DataFrame."""
        transformer = DataTransformer()
        empty_df = pd.DataFrame([])
        
        with pytest.raises(ValueError) as exc_info:
            transformer.transform(empty_df, basic_config)
        assert "Empty DataFrame provided" in str(exc_info.value)

    def test_missing_identifier_field(self, sample_data):
        """Test configuration with missing identifier field."""
        config = TransformerConfig(
            identifier_field="NonexistentField",
            target_fields=["Customer ID", "Name"]
        )
        transformer = DataTransformer()
        
        result = transformer.transform(sample_data, config)
        assert len(result) == 0

    def test_date_conversion(self, sample_data, basic_config):
        """Test date field conversion."""
        transformer = DataTransformer()
        result = transformer.transform(sample_data, basic_config)
        
        assert isinstance(result['order_date'].iloc[0], pd.Timestamp)
        assert result['order_date'].iloc[0] == pd.Timestamp('2024-01-15')

    def test_field_aliases(self, sample_data):
        """Test field alias functionality."""
        config = TransformerConfig(
            identifier_field="Customer ID",
            target_fields=["Customer ID", "Name", "Total"],
            field_aliases={
                "Customer Number": "Customer ID",
                "Full Name": "Name",
                "Amount": "Total"
            }
        )
        
        transformer = DataTransformer()
        result = transformer.transform(sample_data, config)
        
        assert 'customer_id' in result.columns
        assert len(result) == 2

    def test_search_radius(self, sample_data):
        """Test search radius configuration."""
        config = TransformerConfig(
            identifier_field="Customer ID",
            target_fields=["Customer ID", "Name", "Total"],
            search_radius=2  # Small search radius
        )
        
        transformer = DataTransformer()
        result = transformer.transform(sample_data, config)
        
        # Should only find fields within 2 rows of identifier
        assert pd.isna(result['total'].iloc[0])

    @pytest.mark.parametrize("test_input,expected", [
        ("Test Value", "test_value"),
        ("Test & Value", "test_and_value"),
        ("Test:Value", "testvalue"),
        ("Test  Value", "test_value")
    ])
    def test_column_name_cleaning(self, test_input, expected):
        """Test column name cleaning with various inputs."""
        transformer = DataTransformer()
        result = transformer._clean_column_names(pd.Index([test_input]))
        assert result[0] == expected

class TestDataValidator:
    @pytest.fixture
    def sample_validation_data(self):
        """Create sample data for validation testing."""
        return pd.DataFrame({
            'id': ['1', '2', '3'],
            'name': ['John', 'Jane', 'Bob'],
            'amount': [100, 200, 300],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })

    def test_required_columns_validation(self, sample_validation_data):
        """Test validation of required columns."""
        validator = DataValidator()
        rules = {
            "required_columns": ["id", "name", "amount"]
        }
        
        result = validator.validate(sample_validation_data, rules)
        assert result.is_valid

        # Test with missing column
        df_missing = sample_validation_data.drop('amount', axis=1)
        result = validator.validate(df_missing, rules)
        assert not result.is_valid
        assert any("Missing required columns" in error for error in result.errors)

    def test_numeric_validation(self, sample_validation_data):
        """Test validation of numeric columns."""
        validator = DataValidator()
        rules = {
            "numeric_columns": ["amount"]
        }
        
        result = validator.validate(sample_validation_data, rules)
        assert result.is_valid

        # Test with non-numeric value
        df_invalid = sample_validation_data.copy()
        df_invalid.loc[0, 'amount'] = 'invalid'
        result = validator.validate(df_invalid, rules)
        assert not result.is_valid

    def test_date_format_validation(self, sample_validation_data):
        """Test validation of date formats."""
        validator = DataValidator()
        rules = {
            "date_format": {
                "date": "%Y-%m-%d"
            }
        }
        
        result = validator.validate(sample_validation_data, rules)
        assert result.is_valid

        # Test with invalid date
        df_invalid = sample_validation_data.copy()
        df_invalid.loc[0, 'date'] = 'invalid-date'
        result = validator.validate(df_invalid, rules)
        assert not result.is_valid

    def test_custom_validation(self, sample_validation_data):
        """Test custom validation function."""
        def validate_amounts(df):
            if (df['amount'] <= 0).any():
                return {
                    'valid': False,
                    'message': 'Negative amounts found'
                }
            return {'valid': True}

        validator = DataValidator()
        rules = {
            "custom_validations": [{
                "function": validate_amounts
            }]
        }
        
        result = validator.validate(sample_validation_data, rules)
        assert result.is_valid

        # Test with invalid data
        df_invalid = sample_validation_data.copy()
        df_invalid.loc[0, 'amount'] = -100
        result = validator.validate(df_invalid, rules)
        assert not result.is_valid

def test_end_to_end_transformation(sample_data, basic_config):
    """Test complete transformation process including validation."""
    # Setup
    transformer = DataTransformer()
    validator = DataValidator()
    
    # Validation rules
    rules = {
        "required_columns": ["Customer ID", "Name", "Total"],
        "numeric_columns": ["Total"],
        "date_format": {
            "Order Date": "%Y-%m-%d"
        }
    }
    
    # Validate
    validation_result = validator.validate(sample_data, rules)
    assert validation_result.is_valid
    
    # Transform
    result = transformer.transform(sample_data, basic_config)
    
    # Verify results
    assert len(result) == 2
    assert all(col in result.columns for col in ['customer_id', 'name', 'total'])
    assert result['total'].dtype in [np.float64, np.float32]
    assert isinstance(result['order_date'].iloc[0], pd.Timestamp)
