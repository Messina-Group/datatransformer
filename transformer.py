# hierarchical_transformer/
# __init__.py
from .processor import DataTransformer

# processor.py
import pandas as pd
import logging
from typing import List, Dict, Optional, Set
from dataclasses import dataclass

@dataclass
class TransformerConfig:
    """Configuration for data transformation."""
    skip_rows: int = 0
    drop_columns: List[int] = None
    date_columns: List[str] = None
    identifier_field: str = None  # Field that indicates start of a new record
    target_fields: List[str] = None  # Fields to extract
    field_aliases: Dict[str, str] = None  # Map alternative field names to standard names
    search_radius: int = 10  # How many rows to look ahead/behind for values
    column_search_radius: int = 5  # How many columns to look right for values

class DataTransformer:
    """
    A class for transforming hierarchical spreadsheet data into normalized tabular format.
    
    This transformer handles complex data structures where each record spans multiple rows
    and columns, converting them into a normalized pandas DataFrame format suitable for
    analysis or database storage.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the transformer with optional custom logger.
        
        Args:
            logger: Custom logger instance. If None, creates a default logger.
        """
        self.logger = logger or self._setup_default_logger()

    @staticmethod
    def _setup_default_logger() -> logging.Logger:
        """Create and configure a default logger."""
        logger = logging.getLogger("DataTransformer")
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def transform(self, 
                 df: pd.DataFrame,
                 config: TransformerConfig) -> pd.DataFrame:
        """
        Transform hierarchical data into normalized tabular format.
        
        Args:
            df: Input DataFrame containing hierarchical data
            config: Transformation configuration parameters
            
        Returns:
            Processed DataFrame with normalized structure
        """
        self.logger.info("Starting data transformation")
        
        # Validate configuration
        self._validate_config(config)
        
        # Apply initial transformations
        df = self._apply_initial_transformations(df, config)
        
        # Extract records
        records = self._extract_records(df, config)
        
        # Create and clean final DataFrame
        processed_df = self._create_final_dataframe(records, config)
        
        return processed_df

    def _validate_config(self, config: TransformerConfig):
        """Validate the configuration parameters."""
        if not config.identifier_field:
            raise ValueError("identifier_field must be specified in config")
        if not config.target_fields:
            raise ValueError("target_fields must be specified in config")

    def _apply_initial_transformations(self, 
                                     df: pd.DataFrame, 
                                     config: TransformerConfig) -> pd.DataFrame:
        """Apply initial data transformations based on configuration."""
        if config.skip_rows:
            df = df.iloc[config.skip_rows:].reset_index(drop=True)
            self.logger.info(f"Skipped first {config.skip_rows} rows")
            
        if config.drop_columns:
            df = df.drop(df.columns[config.drop_columns], axis=1)
            self.logger.info(f"Dropped specified columns: {config.drop_columns}")
            
        return df

    def _extract_records(self, 
                        df: pd.DataFrame, 
                        config: TransformerConfig) -> List[Dict]:
        """Extract individual records from the DataFrame."""
        records = []
        i = 0
        total_rows = len(df)
        
        while i < total_rows:
            # Check for identifier field in any column
            if self._is_record_start(df.iloc[i], config):
                record = self._extract_single_record(df, i, total_rows, config)
                if record:
                    records.append(record)
                
                # Skip to next record section
                i = self._find_next_record_start(df, i + 1, total_rows, config)
            else:
                i += 1
                
        self.logger.info(f"Extracted {len(records)} records")
        return records

    def _is_record_start(self, 
                        row: pd.Series, 
                        config: TransformerConfig) -> bool:
        """Check if row contains the identifier field."""
        identifier = config.identifier_field
        aliases = config.field_aliases or {}
        valid_identifiers = {identifier} | {aliases.get(identifier, '')}
        return any(str(val).strip() in valid_identifiers for val in row if pd.notnull(val))

    def _extract_single_record(self, 
                             df: pd.DataFrame, 
                             start_idx: int,
                             total_rows: int,
                             config: TransformerConfig) -> Dict:
        """Extract data for a single record."""
        record = {}
        
        for field in config.target_fields:
            field_aliases = {field}
            if config.field_aliases:
                field_aliases.update(k for k, v in config.field_aliases.items() if v == field)
            
            # Search nearby rows for the field
            value = None
            for row_offset in range(config.search_radius):
                row_idx = start_idx + row_offset
                if row_idx >= total_rows:
                    break
                    
                value = self._find_field_value(df, row_idx, field_aliases, config)
                if value is not None:
                    record[field] = value
                    break
                        
        return record

    def _find_field_value(self, 
                         df: pd.DataFrame, 
                         row_idx: int, 
                         field_aliases: Set[str],
                         config: TransformerConfig) -> Optional[str]:
        """Find value for a given field in the DataFrame."""
        row = df.iloc[row_idx]
        
        # Find field column
        field_col = None
        for col in df.columns:
            if pd.notnull(row[col]) and str(row[col]).strip() in field_aliases:
                field_col = col
                break
                
        if field_col is None:
            return None
            
        col_idx = df.columns.get_loc(field_col)
        
        # Search rightwards in same row
        for col_offset in range(1, min(config.column_search_radius, df.shape[1] - col_idx)):
            value = df.iloc[row_idx, col_idx + col_offset]
            if pd.notnull(value) and str(value).strip():
                return str(value).strip()
                
        # Search downwards if not found in same row
        for k in range(row_idx + 1, min(row_idx + config.search_radius, len(df))):
            value = df.iloc[k, col_idx]
            if pd.notnull(value) and str(value).strip():
                return str(value).strip()
                
        return None

    def _find_next_record_start(self,
                               df: pd.DataFrame,
                               start_idx: int,
                               total_rows: int,
                               config: TransformerConfig) -> int:
        """Find the starting index of the next record."""
        for i in range(start_idx, total_rows):
            if self._is_record_start(df.iloc[i], config):
                return i
        return total_rows

    def _create_final_dataframe(self, 
                               records: List[Dict], 
                               config: TransformerConfig) -> pd.DataFrame:
        """Create and clean final DataFrame from extracted records."""
        df = pd.DataFrame(records)
        
        # Clean column names
        df.columns = self._clean_column_names(df.columns)
        
        # Convert date columns
        if config.date_columns:
            for col in config.date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
        # Drop empty columns
        df = df.dropna(axis=1, how='all')
        
        self.logger.info("Finished processing DataFrame")
        return df

    @staticmethod
    def _clean_column_names(columns: pd.Index) -> pd.Index:
        """Clean and standardize column names."""
        return (columns
                .str.strip()
                .str.replace('\n', '', regex=False)
                .str.lower()
                .str.replace(' ', '_')
                .str.replace('__', '_')
                .str.replace('/', '_')
                .str.replace(':', '', regex=False)
                .str.replace('__+', '_', regex=True))
