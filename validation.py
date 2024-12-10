# hierarchical_transformer/validation.py
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime

@dataclass
class ValidationResult:
    """Contains the results of data validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]

class DataValidator:
    """Validates data before transformation."""
    
    def __init__(self):
        self.errors = []
        self.warnings = []

    def validate(self, 
                df: pd.DataFrame, 
                rules: Dict[str, Any]) -> ValidationResult:
        """
        Validate DataFrame against specified rules.
        
        Args:
            df: Input DataFrame to validate
            rules: Dictionary of validation rules
            
        Returns:
            ValidationResult object containing validation status and messages
        """
        self.errors = []
        self.warnings = []
        
        # Check required columns
        if "required_columns" in rules:
            self._validate_required_columns(df, rules["required_columns"])
            
        # Check date formats
        if "date_format" in rules:
            self._validate_dates(df, rules["date_format"])
            
        # Check numeric columns
        if "numeric_columns" in rules:
            self._validate_numeric_columns(df, rules["numeric_columns"])
            
        # Check minimum values
        if "min_value" in rules:
            self._validate_min_values(df, rules["min_value"])
            
        # Check maximum values
        if "max_value" in rules:
            self._validate_max_values(df, rules["max_value"])
            
        # Check unique constraints
        if "unique_columns" in rules:
            self._validate_unique_columns(df, rules["unique_columns"])
            
        # Check custom validation functions
        if "custom_validations" in rules:
            self._run_custom_validations(df, rules["custom_validations"])

        return ValidationResult(
            is_valid=len(self.errors) == 0,
            errors=self.errors,
            warnings=self.warnings
        )

    def _validate_required_columns(self, 
                                 df: pd.DataFrame, 
                                 required_columns: List[str]):
        """Check if all required columns are present."""
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.errors.append(f"Missing required columns: {missing_columns}")

    def _validate_dates(self, 
                       df: pd.DataFrame, 
                       date_formats: Dict[str, str]):
        """Validate date formats in specified columns."""
        for column, date_format in date_formats.items():
            if column in df.columns:
                invalid_dates = []
                for idx, value in df[column].items():
                    if pd.notnull(value):
                        try:
                            if not isinstance(value, (datetime, pd.Timestamp)):
                                datetime.strptime(str(value), date_format)
                        except ValueError:
                            invalid_dates.append(f"Row {idx}: {value}")
                
                if invalid_dates:
                    self.errors.append(
                        f"Invalid date format in column '{column}'. "
                        f"Expected format: {date_format}. "
                        f"Found invalid values: {invalid_dates}"
                    )

    def _validate_numeric_columns(self, 
                                df: pd.DataFrame, 
                                numeric_columns: List[str]):
        """Validate numeric columns contain only numbers."""
        for column in numeric_columns:
            if column in df.columns:
                non_numeric = df[
                    ~df[column].apply(
                        lambda x: pd.isna(x) or str(x).replace('.', '').replace('-', '').isdigit()
                    )
                ]
                if not non_numeric.empty:
                    self.errors.append(
                        f"Non-numeric values found in column '{column}' "
                        f"at rows: {non_numeric.index.tolist()}"
                    )

    def _validate_min_values(self, 
                           df: pd.DataFrame, 
                           min_values: Dict[str, float]):
        """Validate minimum values for specified columns."""
        for column, min_value in min_values.items():
            if column in df.columns:
                invalid_values = df[df[column] < min_value]
                if not invalid_values.empty:
                    self.errors.append(
                        f"Values below minimum ({min_value}) found in column '{column}' "
                        f"at rows: {invalid_values.index.tolist()}"
                    )

    def _validate_max_values(self, 
                           df: pd.DataFrame, 
                           max_values: Dict[str, float]):
        """Validate maximum values for specified columns."""
        for column, max_value in max_values.items():
            if column in df.columns:
                invalid_values = df[df[column] > max_value]
                if not invalid_values.empty:
                    self.errors.append(
                        f"Values above maximum ({max_value}) found in column '{column}' "
                        f"at rows: {invalid_values.index.tolist()}"
                    )

    def _validate_unique_columns(self, 
                               df: pd.DataFrame, 
                               unique_columns: List[str]):
        """Validate uniqueness constraints."""
        for column in unique_columns:
            if column in df.columns:
                duplicates = df[df[column].duplicated()]
                if not duplicates.empty:
                    self.errors.append(
                        f"Duplicate values found in column '{column}' "
                        f"at rows: {duplicates.index.tolist()}"
                    )

    def _run_custom_validations(self, 
                              df: pd.DataFrame, 
                              custom_validations: List[Dict[str, Any]]):
        """Run custom validation functions."""
        for validation in custom_validations:
            func = validation.get('function')
            if func:
                result = func(df)
                if not result.get('valid', True):
                    self.errors.append(result.get('message', 'Custom validation failed'))
