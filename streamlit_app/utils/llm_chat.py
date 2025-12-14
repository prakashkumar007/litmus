"""
Chalk and Duster - LLM Chat Utilities for Streamlit

Provides conversation management and LLM interaction for the chatbot.
Includes DDL parsing and rule generation for Great Expectations and Evidently.
"""

import logging
import os
import re
from typing import Dict, List, Optional, TypedDict

import httpx

logger = logging.getLogger(__name__)


# Ollama configuration
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "tinyllama")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))


class ColumnInfo(TypedDict, total=False):
    """Type definition for parsed column information."""
    name: str
    type: str
    nullable: bool
    primary_key: bool
    unique: bool
    check_constraint: Optional[str]
    enum_values: List[str]
    default: Optional[str]


class ParsedDDL(TypedDict, total=False):
    """Type definition for parsed DDL result."""
    table_name: Optional[str]
    database: Optional[str]
    schema: Optional[str]
    columns: List[ColumnInfo]
    primary_keys: List[str]
    foreign_keys: List[str]
    unique_constraints: List[str]


SYSTEM_PROMPT = """You are an AI assistant for Chalk and Duster, an enterprise data quality and drift monitoring platform.

Your role is to help users:
1. Set up their tenant (organization) in the system
2. Create Snowflake connections
3. Parse DDL (Data Definition Language) to understand table schemas
4. Generate data quality rules using Great Expectations format
5. Generate drift monitoring rules using Evidently format
6. Schedule and trigger quality checks and drift detection

Be professional, helpful, and concise. When asking for information, be clear about what you need.
When generating YAML configurations, explain what each rule does.

IMPORTANT CONVERSATION RULES:
- If the user is new, ask if they have an existing tenant ID
- If they don't have a tenant ID, guide them through creating one (need: name, slug)
- After tenant is set, ask about Snowflake connection details
- After connection is set, ask for DDL (CREATE TABLE statement) to generate rules
- Always ask for confirmation before creating anything in the database
- Be conversational and helpful, don't just list requirements

When generating quality YAML, use this Great Expectations format:
```yaml
expectation_suite_name: suite_name
expectations:
  - expectation_type: expect_column_values_to_not_be_null
    kwargs:
      column: column_name
```

When generating drift YAML, use this Evidently format:
```yaml
time_travel_days: 1
monitors:
  - name: monitor_name
    type: distribution
    column: column_name
    threshold: 0.1
```
"""


def chat_with_ollama(
    messages: List[Dict[str, str]],
    temperature: float = 0.7,
) -> str:
    """
    Send messages to Ollama and get a response.

    Args:
        messages: List of message dicts with 'role' and 'content' keys
        temperature: Sampling temperature (0.0-1.0)

    Returns:
        LLM response text or error message
    """
    # Convert messages to a single prompt
    prompt_parts: List[str] = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role == "system":
            prompt_parts.append(f"System: {content}")
        elif role == "user":
            prompt_parts.append(f"User: {content}")
        elif role == "assistant":
            prompt_parts.append(f"Assistant: {content}")

    prompt = "\n\n".join(prompt_parts) + "\n\nAssistant:"

    try:
        with httpx.Client(timeout=OLLAMA_TIMEOUT) as client:
            response = client.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": 2048,
                    },
                },
            )

            if response.status_code == 200:
                data = response.json()
                return data.get("response", "I apologize, but I couldn't generate a response.")
            else:
                logger.warning(f"Ollama returned status {response.status_code}")
                return f"Error communicating with LLM: {response.status_code}"

    except httpx.TimeoutException:
        logger.warning("Ollama request timed out")
        return "The request timed out. Please try again."
    except Exception as e:
        logger.exception("Error calling Ollama")
        return f"Error: {str(e)}"


def extract_yaml_from_response(response: str, yaml_type: str = "quality") -> Optional[str]:
    """
    Extract YAML block from LLM response.

    Args:
        response: LLM response text
        yaml_type: Type of YAML to extract ("quality" or "drift")

    Returns:
        Extracted YAML string or None if not found
    """
    # Try to find labeled code block
    patterns = [
        rf"```(?:yaml)?\s*#?\s*{yaml_type}[_\s]?yaml\s*\n(.*?)```",
        rf"{yaml_type}[_\s]?yaml[:\s]*\n```(?:yaml)?\s*\n(.*?)```",
        r"```(?:yaml)?\s*\n(.*?)```",
    ]

    for pattern in patterns:
        match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return None


def parse_ddl(ddl: str) -> ParsedDDL:
    """
    Parse a CREATE TABLE DDL statement to extract schema information.

    Extracts table name, columns, data types, and constraints including:
    - NOT NULL constraints
    - PRIMARY KEY (inline and table-level)
    - UNIQUE constraints
    - CHECK constraints with ENUM values
    - DEFAULT values

    Args:
        ddl: SQL CREATE TABLE statement

    Returns:
        ParsedDDL with table info and column definitions
    """
    result: ParsedDDL = {
        "table_name": None,
        "database": None,
        "schema": None,
        "columns": [],
        "primary_keys": [],
        "foreign_keys": [],
        "unique_constraints": [],
    }

    # Extract table name (handles database.schema.table format)
    table_match = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        ddl,
        re.IGNORECASE
    )
    if table_match:
        full_name = table_match.group(1).strip('"').strip("'").strip("`")
        parts = full_name.split(".")
        if len(parts) == 3:
            result["database"], result["schema"], result["table_name"] = parts
        elif len(parts) == 2:
            result["schema"], result["table_name"] = parts
        else:
            result["table_name"] = parts[0]

    # Extract table-level PRIMARY KEY constraint
    pk_match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", ddl, re.IGNORECASE)
    if pk_match:
        pk_cols = [c.strip().strip('"').strip("'").strip("`") for c in pk_match.group(1).split(",")]
        result["primary_keys"].extend(pk_cols)

    # Extract columns
    columns_match = re.search(r"\((.*)\)", ddl, re.DOTALL)
    if columns_match:
        columns_str = columns_match.group(1)
        # Split by comma, but be careful of commas inside parentheses
        column_defs = re.split(r",\s*(?![^()]*\))", columns_str)

        for col_def in column_defs:
            col_def = col_def.strip()
            col_def_upper = col_def.upper()
            if not col_def:
                continue
            # Skip table-level constraints
            if re.match(r"^\s*(PRIMARY\s+KEY\s*\(|FOREIGN\s+KEY|UNIQUE\s*\(|CHECK\s*\(|CONSTRAINT)", col_def, re.IGNORECASE):
                continue

            # Parse column: name type [constraints]
            col_match = re.match(r"[\"'`]?(\w+)[\"'`]?\s+(\w+(?:\([^)]+\))?)", col_def, re.IGNORECASE)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2).upper()

                column_info = {
                    "name": col_name,
                    "type": col_type,
                    "nullable": "NOT NULL" not in col_def_upper,
                    "primary_key": False,
                    "unique": False,
                    "check_constraint": None,
                    "enum_values": [],
                    "default": None,
                }

                # Check for inline PRIMARY KEY
                if "PRIMARY KEY" in col_def_upper or "PRIMARY_KEY" in col_def_upper:
                    column_info["primary_key"] = True
                    column_info["nullable"] = False  # PKs are implicitly NOT NULL
                    if col_name not in result["primary_keys"]:
                        result["primary_keys"].append(col_name)

                # Check for UNIQUE
                if re.search(r"\bUNIQUE\b", col_def_upper):
                    column_info["unique"] = True
                    result["unique_constraints"].append(col_name)

                # Extract ENUM values from CHECK constraint or ENUM keyword
                # Pattern 1: ENUM (val1, val2, val3) or ENUM('val1', 'val2')
                enum_match = re.search(r"ENUM\s*\(([^)]+)\)", col_def, re.IGNORECASE)
                if enum_match:
                    enum_vals = enum_match.group(1)
                    values = re.findall(r"['\"]?([^',\"]+)['\"]?", enum_vals)
                    column_info["enum_values"] = [v.strip() for v in values if v.strip()]

                # Pattern 2: CHECK (column IN ('val1', 'val2')) or CHECK (column = 'val1' OR ...)
                check_match = re.search(r"CHECK\s*\(([^)]+)\)", col_def, re.IGNORECASE)
                if check_match:
                    check_content = check_match.group(1)
                    column_info["check_constraint"] = check_content.strip()

                    # Try to extract IN values: column IN ('a', 'b', 'c')
                    in_match = re.search(r"IN\s*\(([^)]+)\)", check_content, re.IGNORECASE)
                    if in_match and not column_info["enum_values"]:
                        in_vals = in_match.group(1)
                        values = re.findall(r"['\"]([^'\"]+)['\"]", in_vals)
                        column_info["enum_values"] = [v.strip() for v in values if v.strip()]

                # Extract DEFAULT value
                default_match = re.search(r"DEFAULT\s+([^\s,]+|'[^']*'|\"[^\"]*\")", col_def, re.IGNORECASE)
                if default_match:
                    column_info["default"] = default_match.group(1).strip("'\"")

                result["columns"].append(column_info)

    return result


def generate_quality_rules(parsed_ddl: ParsedDDL) -> str:
    """
    Generate Great Expectations quality YAML based on parsed DDL constraints.

    Creates expectations for:
    - NOT NULL columns
    - PRIMARY KEY uniqueness
    - UNIQUE constraints
    - ENUM/CHECK value sets
    - Numeric range validations
    - VARCHAR length limits

    Args:
        parsed_ddl: Parsed DDL result from parse_ddl()

    Returns:
        YAML string with Great Expectations configuration
    """
    table_name = parsed_ddl.get("table_name", "table")
    columns = parsed_ddl.get("columns", [])

    expectations = []

    for col in columns:
        col_name = col["name"]
        col_type = col["type"]

        # 1. NOT NULL constraint -> expect_column_values_to_not_be_null
        if not col["nullable"]:
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col_name},
                "reason": f"Column '{col_name}' is defined as NOT NULL"
            })

        # 2. PRIMARY KEY -> uniqueness + not null
        if col["primary_key"]:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": col_name},
                "reason": f"Column '{col_name}' is a PRIMARY KEY (must be unique)"
            })

        # 3. UNIQUE constraint -> expect_column_values_to_be_unique
        if col["unique"] and not col["primary_key"]:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": col_name},
                "reason": f"Column '{col_name}' has UNIQUE constraint"
            })

        # 4. ENUM/CHECK with values -> expect_column_values_to_be_in_set
        if col["enum_values"]:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": col_name, "value_set": col["enum_values"]},
                "reason": f"Column '{col_name}' has allowed values: {col['enum_values']}"
            })

        # 5. Type-specific validations
        type_upper = col_type.upper()

        # Numeric types - check for reasonable ranges
        if any(t in type_upper for t in ["INT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "NUMBER"]):
            # For amount/price/quantity columns, check positive values
            if any(kw in col_name.lower() for kw in ["amount", "price", "quantity", "count", "total", "balance"]):
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {"column": col_name, "min_value": 0},
                    "reason": f"Column '{col_name}' is a numeric field that should be non-negative"
                })

        # Date/Timestamp types
        if any(t in type_upper for t in ["DATE", "TIME", "TIMESTAMP"]):
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col_name},
                "reason": f"Column '{col_name}' is a date/timestamp and should be validated"
            })

        # String length validation for VARCHAR
        varchar_match = re.search(r"VARCHAR\((\d+)\)", type_upper)
        if varchar_match:
            max_len = int(varchar_match.group(1))
            expectations.append({
                "expectation_type": "expect_column_value_lengths_to_be_between",
                "kwargs": {"column": col_name, "max_value": max_len},
                "reason": f"Column '{col_name}' has max length of {max_len}"
            })

    # Build YAML string
    yaml_lines = [
        f"expectation_suite_name: {table_name}_quality_suite",
        "expectations:"
    ]

    for exp in expectations:
        yaml_lines.append(f"  # {exp['reason']}")
        yaml_lines.append(f"  - expectation_type: {exp['expectation_type']}")
        yaml_lines.append("    kwargs:")
        for key, value in exp["kwargs"].items():
            if isinstance(value, list):
                yaml_lines.append(f"      {key}:")
                for v in value:
                    yaml_lines.append(f"        - \"{v}\"")
            elif isinstance(value, str):
                yaml_lines.append(f"      {key}: \"{value}\"")
            else:
                yaml_lines.append(f"      {key}: {value}")

    return "\n".join(yaml_lines)


def generate_drift_rules(parsed_ddl: ParsedDDL) -> str:
    """
    Generate Evidently drift YAML based on parsed DDL.

    Creates monitors for:
    - Numeric columns: distribution drift
    - Categorical columns: category distribution
    - Date columns: data freshness/recency
    - Schema changes

    Args:
        parsed_ddl: Parsed DDL result from parse_ddl()

    Returns:
        YAML string with Evidently drift configuration
    """
    table_name = parsed_ddl.get("table_name", "table")
    columns = parsed_ddl.get("columns", [])

    monitors = []

    for col in columns:
        col_name = col["name"]
        col_type = col["type"].upper()

        # Skip primary keys from drift monitoring (IDs shouldn't drift)
        if col["primary_key"] and any(kw in col_name.lower() for kw in ["id", "key"]):
            continue

        # Numeric columns - distribution drift
        if any(t in col_type for t in ["INT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "NUMBER"]):
            monitors.append({
                "name": f"{col_name}_distribution",
                "type": "distribution",
                "column": col_name,
                "threshold": 0.1,
                "reason": f"Monitor distribution drift for numeric column '{col_name}'"
            })

        # Categorical/ENUM columns - value drift
        elif col["enum_values"] or any(t in col_type for t in ["VARCHAR", "CHAR", "TEXT", "STRING"]):
            monitors.append({
                "name": f"{col_name}_category_drift",
                "type": "category",
                "column": col_name,
                "threshold": 0.15,
                "reason": f"Monitor category distribution for column '{col_name}'"
            })

        # Date columns - check for freshness/recency
        elif any(t in col_type for t in ["DATE", "TIME", "TIMESTAMP"]):
            if any(kw in col_name.lower() for kw in ["created", "updated", "modified", "date"]):
                monitors.append({
                    "name": f"{col_name}_recency",
                    "type": "data_recency",
                    "column": col_name,
                    "threshold": 86400,  # 24 hours in seconds
                    "reason": f"Monitor data freshness for date column '{col_name}'"
                })

    # Schema drift monitor
    monitors.append({
        "name": "schema_drift",
        "type": "schema",
        "threshold": 0.0,
        "reason": "Detect any schema changes (added/removed/modified columns)"
    })

    # Build YAML string
    yaml_lines = [
        f"# Drift monitoring configuration for {table_name}",
        "time_travel_days: 1",
        "monitors:"
    ]

    for mon in monitors:
        yaml_lines.append(f"  # {mon['reason']}")
        yaml_lines.append(f"  - name: {mon['name']}")
        yaml_lines.append(f"    type: {mon['type']}")
        if "column" in mon:
            yaml_lines.append(f"    column: {mon['column']}")
        yaml_lines.append(f"    threshold: {mon['threshold']}")

    return "\n".join(yaml_lines)

