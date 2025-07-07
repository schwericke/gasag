# Snowflake Connection Utility

This utility provides a reusable way to connect to Snowflake databases across your project.

## Setup

### 1. Install Dependencies
Make sure you have the required packages installed:
```bash
pip install snowflake-connector-python pandas
```

### 2. Environment Variables
Set up your Snowflake connection parameters as environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
export SNOWFLAKE_ROLE="your_role"
```

Or create a `.env` file in your project root:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
```

## Usage

### Basic Usage
```python
from utils.snowflake_connection import get_connection, close_connection

# Get connection using environment variables
conn = get_connection()

# Execute queries
cursor = conn.cursor()
cursor.execute("SELECT * FROM your_table LIMIT 10")
results = cursor.fetchall()

# Close cursor and connection
cursor.close()
close_connection(conn)
```

### With Explicit Parameters
```python
conn = get_connection(
    account="your_account",
    user="your_user",
    password="your_password",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema"
)
```

### With Pandas
```python
import pandas as pd
from utils.snowflake_connection import get_connection, close_connection

conn = get_connection()
df = pd.read_sql("SELECT * FROM your_table", conn)
close_connection(conn)
```

### Test Connection
```python
from utils.snowflake_connection import test_connection

if test_connection():
    print("Connection successful!")
else:
    print("Connection failed!")
```

## Functions

### `get_connection()`
Returns a connected Snowflake connection object.

**Parameters:**
- `account` (str, optional): Snowflake account identifier
- `user` (str, optional): Snowflake username
- `password` (str, optional): Snowflake password
- `warehouse` (str, optional): Snowflake warehouse name
- `database` (str, optional): Snowflake database name
- `schema` (str, optional): Snowflake schema name
- `role` (str, optional): Snowflake role name

**Returns:**
- `snowflake.connector.SnowflakeConnection`: Connected Snowflake connection

### `test_connection()`
Tests the connection by executing a simple query.

**Returns:**
- `bool`: True if connection successful, False otherwise

### `close_connection(conn)`
Safely closes a Snowflake connection.

**Parameters:**
- `conn`: Snowflake connection object to close

## Error Handling

The utility includes comprehensive error handling:
- Validates required connection parameters
- Logs connection attempts and errors
- Provides clear error messages for missing parameters
- Safely handles connection closure

## Examples

See `example_usage.py` for complete usage examples.
