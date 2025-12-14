"""
Chalk and Duster - LLM Prompt Templates

Prompts for generating Great Expectations and Evidently YAML configurations.
"""

# System prompt for YAML generation
YAML_GENERATOR_SYSTEM = """You are an expert data quality engineer. Your task is to generate
Great Expectations and Evidently YAML configurations for data quality checks and drift monitoring.

For data quality checks, use Great Expectations format:
```yaml
expectation_suite_name: my_suite_name
expectations:
  - expectation_type: expect_table_row_count_to_be_between
    kwargs:
      min_value: 1
  - expectation_type: expect_column_values_to_not_be_null
    kwargs:
      column: column_name
  - expectation_type: expect_column_values_to_be_unique
    kwargs:
      column: id_column
  - expectation_type: expect_column_values_to_be_in_set
    kwargs:
      column: status_column
      value_set: ["active", "inactive", "pending"]
  - expectation_type: expect_column_values_to_be_between
    kwargs:
      column: numeric_column
      min_value: 0
      max_value: 100
  - expectation_type: expect_column_values_to_match_regex
    kwargs:
      column: email_column
      regex: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
```

For drift monitoring, use Evidently format:
```yaml
monitors:
  - name: schema_drift_monitor
    type: schema
  - name: volume_drift_monitor
    type: volume
    threshold: 0.2
  - name: dataset_drift_monitor
    type: dataset
    threshold: 0.3
  - name: column_drift_monitor
    type: distribution
    column: column_name
    threshold: 0.1
    stattest: ks
```

Valid statistical tests (stattest): ks, chisquare, z, wasserstein, psi, jensenshannon

Always include practical, meaningful checks based on the user's description.
Do not include checks that don't make sense for the described data.
"""

# Prompt template for YAML generation
YAML_GENERATOR_PROMPT = """Generate data quality and drift monitoring YAML for the following requirements:

{description}

{table_context}
{schema_context}

Please generate:
{include_quality}
{include_drift}

Output the YAML configurations in separate code blocks labeled 'quality_yaml' and 'drift_yaml'.
Include a brief explanation of what each check does.
"""

# System prompt for alert enhancement
ALERT_ENHANCER_SYSTEM = """You are a data quality expert helping teams understand data issues.
Your task is to analyze data quality failures and provide:
1. A clear, concise summary of what went wrong
2. Possible root causes
3. Recommended actions to fix the issues
4. A severity assessment

Be specific and actionable. Avoid jargon when possible.
Format your response for Slack (use *bold* for emphasis, bullet points for lists).
"""

# Prompt template for alert enhancement
ALERT_ENHANCER_PROMPT = """Analyze these data quality failures for dataset "{dataset_name}":

Run ID: {run_id}

Failures:
{failures}

{context}

Please provide:
1. A brief summary (2-3 sentences)
2. Possible root causes (bullet points)
3. Recommended actions (bullet points)
4. Overall severity (critical/warning/info)
5. A formatted Slack message ready to send
"""

# System prompt for drift explanation
DRIFT_EXPLAINER_SYSTEM = """You are a data analyst explaining data drift to stakeholders.
Your task is to explain what changed in the data in plain English.
Focus on business impact and actionable insights.
Avoid technical jargon when possible.
"""

# Prompt template for drift explanation
DRIFT_EXPLAINER_PROMPT = """Explain the following drift detection results for dataset "{dataset_name}":

Drift Results:
{drift_results}

{baseline_context}

Please provide:
1. A plain English summary of what changed
2. List of specific changes with explanations
3. Assessment of potential business impact
4. Recommendations for handling the drift
"""


def format_yaml_prompt(
    description: str,
    table_name: str = None,
    schema_info: dict = None,
    include_quality: bool = True,
    include_drift: bool = True,
) -> str:
    """Format the YAML generation prompt."""
    table_context = f"Table name: {table_name}" if table_name else ""
    
    schema_context = ""
    if schema_info:
        columns = schema_info.get("columns", [])
        if columns:
            schema_context = "Schema:\n" + "\n".join(
                f"  - {col['name']}: {col['type']}" for col in columns
            )
    
    include_quality_text = "- Great Expectations YAML (expectation_suite_name and expectations)" if include_quality else ""
    include_drift_text = "- Evidently drift monitoring YAML (monitors)" if include_drift else ""
    
    return YAML_GENERATOR_PROMPT.format(
        description=description,
        table_context=table_context,
        schema_context=schema_context,
        include_quality=include_quality_text,
        include_drift=include_drift_text,
    )


def format_alert_prompt(
    dataset_name: str,
    run_id: str,
    failures: list,
    context: dict = None,
) -> str:
    """Format the alert enhancement prompt."""
    failures_text = "\n".join(
        f"- {f.get('check_name', 'Unknown')}: {f.get('message', 'No message')}"
        for f in failures
    )
    
    context_text = ""
    if context:
        context_text = f"\nAdditional context:\n{context}"
    
    return ALERT_ENHANCER_PROMPT.format(
        dataset_name=dataset_name,
        run_id=run_id,
        failures=failures_text,
        context=context_text,
    )


def format_drift_prompt(
    dataset_name: str,
    drift_results: list,
    baseline_info: dict = None,
) -> str:
    """Format the drift explanation prompt."""
    results_text = "\n".join(
        f"- {r.get('monitor_name', 'Unknown')}: {r.get('message', 'No message')}"
        for r in drift_results
    )
    
    baseline_text = ""
    if baseline_info:
        baseline_text = f"\nBaseline information:\n{baseline_info}"
    
    return DRIFT_EXPLAINER_PROMPT.format(
        dataset_name=dataset_name,
        drift_results=results_text,
        baseline_context=baseline_text,
    )

