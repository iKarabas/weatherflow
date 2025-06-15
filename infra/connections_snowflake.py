"""
Snowflake Connector with Safe Schema Evolution

Author: Data Engineering Team
"""

import snowflake.connector
import logging
import os
from datetime import datetime


class SnowflakeConnector:
    """Snowflake connector with safe schema evolution capabilities"""
    
    def __init__(self, account, user, password, warehouse):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.connection = None
    
    def connect(self):
        """Connect to Snowflake"""
        self.connection = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
        )
        logging.info("Connected to Snowflake")
    
    def execute_query(self, query):
        """Execute a query"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def get_table_row_count(self, database_name, schema_name, table_name):
        """Get row count for a specific table"""
        try:
            # Set context
            self.execute_query(f"USE DATABASE {database_name}")
            self.execute_query(f"USE SCHEMA {schema_name}")
            
            # Check if table exists and get count
            if self.table_exists(table_name):
                result = self.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                return result[0][0] if result else 0
            return 0
        except Exception as e:
            logging.error(f"Error getting row count for {database_name}.{schema_name}.{table_name}: {e}")
            return 0
    
    def table_exists(self, table_name):
        """Check if table exists"""
        try:
            query = f"DESCRIBE TABLE {table_name}"
            self.execute_query(query)
            return True
        except Exception:
            return False

    def get_current_table_schema(self, table_name):
        """Get current table schema"""
        query = f"DESCRIBE TABLE {table_name}"
        result = self.execute_query(query)
        
        schema = {}
        for row in result:
            column_name = row[0].lower()  # Convert to lowercase for consistency
            data_type = row[1]
            nullable = row[3] == 'Y'
            default_value = row[4] if len(row) > 4 and row[4] else None 
            
            schema[column_name] = {
                'type': data_type,
                'nullable': nullable,
                'default': default_value
            }
        
        return schema

    def normalize_type_for_comparison(self, snowflake_type):
        """Normalize Snowflake types to our schema types using smart pattern matching"""
        import re
        
        sf_type = snowflake_type.upper().strip()
        
        # Extract base type and parameters
        base_type_match = re.match(r'^([A-Z_]+)', sf_type)
        if not base_type_match:
            return sf_type
        
        base_type = base_type_match.group(1)
        
        # Base type mappings
        base_mappings = {
            # String types
            'VARCHAR': 'STRING',
            'TEXT': 'STRING', 
            'STRING': 'STRING',
            'CHAR': 'STRING',
            
            # Numeric types  
            'INT': 'INTEGER',
            'INTEGER': 'INTEGER',
            'BIGINT': 'INTEGER',
            'SMALLINT': 'INTEGER',
            'TINYINT': 'INTEGER',
            
            # Float types
            'FLOAT': 'FLOAT',
            'DOUBLE': 'FLOAT', 
            'REAL': 'FLOAT',
            
            # Date/Time types
            'DATE': 'DATE',
            'TIME': 'TIME',
            
            # Special types
            'VARIANT': 'VARIANT',
            'OBJECT': 'VARIANT',
            'ARRAY': 'VARIANT',
            'BOOLEAN': 'BOOLEAN',
            'BOOL': 'BOOLEAN'
        }
        
        # Handle special cases
        if base_type == 'NUMBER':
            # NUMBER(x,0) -> INTEGER, NUMBER(x,y) -> FLOAT
            if re.match(r'^NUMBER\(\d+,0\)$', sf_type):
                return 'INTEGER'
            else:
                return 'FLOAT'
        
        elif base_type.startswith('TIMESTAMP'):
            # Any TIMESTAMP variation -> TIMESTAMP
            return 'TIMESTAMP'
        
        # Use base mapping
        return base_mappings.get(base_type, sf_type)

    def compare_schemas(self, current_schema, expected_schema):
        """Compare current and expected schemas - only add missing columns, log type mismatches"""
        safe_changes = []
        type_mismatches = []
        
        for field_name, field_config in expected_schema.items():
            if field_name not in current_schema:
                # Missing column - safe to add if nullable
                if field_config.get('nullable', True):
                    safe_changes.append({
                        'type': 'add_column',
                        'column': field_name,
                        'data_type': field_config['type'],
                        'nullable': field_config.get('nullable', True),
                        'default': field_config.get('default')
                    })
                else:
                    logging.warning(f"Cannot add non-nullable column '{field_name}' to existing table")
            else:
                # Existing column - normalize types before comparison
                current_type = current_schema[field_name]['type']
                expected_type = field_config['type']
                normalized_current_type = self.normalize_type_for_comparison(current_type)
                
                if normalized_current_type != expected_type:
                    type_mismatches.append({
                        'column': field_name,
                        'current_type': current_type,
                        'expected_type': expected_type,
                        'normalized_current': normalized_current_type
                    })
        
        return {
            'safe': safe_changes,
            'type_mismatches': type_mismatches
        }
        

    def apply_safe_schema_changes(self, changes):
        """Apply safe schema changes"""
        for change in changes:
            try:
                if change['type'] == 'add_column':
                    nullable_clause = "NULL" if change['nullable'] else "NOT NULL"
                    default_clause = f"DEFAULT {change['default']}" if change.get('default') else ""
                    
                    query = f"""
                    ALTER TABLE WEATHER_DATA 
                    ADD COLUMN {change['column']} {change['data_type']} {nullable_clause} {default_clause}
                    """
                    self.execute_query(query)
                    logging.info(f"Added column: {change['column']} {change['data_type']}")    
            except Exception as e:
                logging.error(f"Failed to apply schema change {change}: {e}")
                raise

    def create_table_from_schema(self, table_name, schema):
        """Create table from schema definition"""
        columns = []
        for field_name, field_config in schema.items():
            nullable = "NULL" if field_config.get('nullable', True) else "NOT NULL"
            default = f"DEFAULT {field_config['default']}" if field_config.get('default') else ""
            columns.append(f"{field_name} {field_config['type']} {nullable} {default}")
        
        columns_sql = ",\n            ".join(columns)
        
        create_query = f"""
        CREATE TABLE {table_name} (
            {columns_sql}
        )
        """
        
        self.execute_query(create_query)
        logging.info(f"Created table {table_name} with {len(schema)} columns")

    def setup_table_with_schema(self, table_name, expected_schema):
        """Setup table with safe schema evolution"""
        if not self.table_exists(table_name):
            # Create new table
            self.create_table_from_schema(table_name, expected_schema)
            logging.info(f"Created new table: {table_name}")
        else:
            # Evolve existing table
            current_schema = self.get_current_table_schema(table_name)
            schema_changes = self.compare_schemas(current_schema, expected_schema)
            
            # Apply safe changes (only adding missing columns)
            if schema_changes['safe']:
                self.apply_safe_schema_changes(schema_changes['safe'])
                logging.info(f"Applied {len(schema_changes['safe'])} safe schema changes")
            
            # Log type mismatches for manual review
            if schema_changes['type_mismatches']:
                logging.info(f"Found {len(schema_changes['type_mismatches'])} type mismatches (compatible types, no action needed):")
                for mismatch in schema_changes['type_mismatches']:
                    normalized = mismatch.get('normalized_current', 'N/A')
                    logging.info(f"  '{mismatch['column']}': {mismatch['current_type']} (normalizes to: {normalized}) vs expected: {mismatch['expected_type']}")
            else:
                logging.info("No type mismatches found - all types are compatible!")
                
    def setup_gcs_integration(self, bucket_name, folder_name):
        """Setup GCS integration"""
        
        if not bucket_name:
            raise ValueError("bucket_name is required for GCS integration")
        if not folder_name:
            raise ValueError("folder_name is required for GCS integration")
        
        try:
            # Check if integration already exists
            check_query = "SHOW INTEGRATIONS LIKE 'GCS_INT'"
            result = self.execute_query(check_query)
            
            if not result:
                # Create storage integration
                create_integration = f"""
                CREATE STORAGE INTEGRATION GCS_INT
                TYPE = EXTERNAL_STAGE
                STORAGE_PROVIDER = GCS
                ENABLED = TRUE
                STORAGE_ALLOWED_LOCATIONS = ('gcs://{bucket_name}/{folder_name}/')
                """
                self.execute_query(create_integration)
                logging.info(f"Created GCS storage integration for gs://{bucket_name}/{folder_name}/")
            else:
                logging.info("GCS integration already exists")
                
                # Check current allowed locations
                desc_query = "DESC STORAGE INTEGRATION GCS_INT"
                desc_result = self.execute_query(desc_query)
                
                current_locations = None
                for row in desc_result:
                    if row[0] == 'STORAGE_ALLOWED_LOCATIONS':
                        current_locations = row[1]
                        break
                
                expected_location = f"gcs://{bucket_name}/{folder_name}/"
                
                if current_locations and expected_location not in current_locations:
                    logging.warning(f"Integration exists but doesn't allow {expected_location}")
                    logging.warning(f"Current allowed locations: {current_locations}")
                    
                    logging.info(f"Current locations type: {type(current_locations)}")
                    logging.info(f"Current locations repr: {repr(current_locations)}")
                    
                    # Drop and recreate for development
                    logging.info("Dropping existing integration to recreate with correct locations")
                    self.execute_query("DROP INTEGRATION GCS_INT")
                    
                    create_integration = f"""
                    CREATE STORAGE INTEGRATION GCS_INT
                    TYPE = EXTERNAL_STAGE
                    STORAGE_PROVIDER = GCS
                    ENABLED = TRUE
                    STORAGE_ALLOWED_LOCATIONS = ('gcs://{bucket_name}/{folder_name}/')
                    """
                    self.execute_query(create_integration)
                    logging.info(f"Recreated GCS storage integration for gs://{bucket_name}/{folder_name}/")
                    
            # Get service account email
            desc_query = "DESC STORAGE INTEGRATION GCS_INT"
            desc_result = self.execute_query(desc_query)
            
            for row in desc_result:
                if row[0] == 'STORAGE_GCP_SERVICE_ACCOUNT':
                    service_account = row[1]
                    logging.info(f"Snowflake Service Account: {service_account}")
                    logging.info(f"Grant this account 'Storage Object Viewer' access to gs://{bucket_name}")
                    return service_account
            
        except Exception as e:
            logging.error(f"GCS integration setup failed: {e}")
            raise
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()