{#
    Builds the canonical ARRAY<STRUCT> data type used by validation columns.

    The validation contract always includes:
      - severity: relative importance of the validation
      - code: stable, machine-readable validation identifier
      - message: human-readable description of the specific validation failure

    Projects can extend the validation payload through
    edu:data_validation.additional_fields without changing the core contract.

    The framework can also add provenance metadata:
      - validator: dbt model that generated the validation
      - applies_to: model whose record receives the validation

    "applies_to" identifies where the validation is attached, not necessarily
    every model the validator referenced while evaluating the rule.
#}
{% macro validation_struct_type() -%}
    {%- set validation_config = var('edu:data_validation', {}) -%}

    array<
        struct<
            severity:string,
            code:string,
            message:string

            {# Add customer/implementation-specific fields to the validation payload. #}
            {% for field, data_type in validation_config.get('additional_fields', {}).items() -%}
            ,{{ field }}:{{ data_type }}
            {%- endfor %}

            {# Include validation provenance only when explicitly configured. #}
            {%- if validation_config.get('include_validator_name', false) -%}
            ,validator:string
            {%- endif %}

            {%- if validation_config.get('include_validator_application', false) -%}
            ,applies_to:string
            {%- endif %}
        >
    >
{%- endmacro %}


{#
    Adds the framework-managed validation columns to a model when data
    validation is enabled.

    The validations column is initialized as an empty array using the canonical
    validation struct type. Validation results can later be merged into this
    array without changing the model''s grain or primary key.

    validation_severity stores the highest applicable severity for the row.
    It begins as null because no validations have been applied when the model
    is initially built.

    When edu:data_validation.enabled is false, this macro emits no SQL. This
    allows the validation framework to remain completely optional without
    changing the schema of implementations that do not enable it.

    The leading comma is intentional so callers can append the macro directly
    to an existing SELECT column list without needing conditional comma logic.
#}
{% macro validation_columns() %}

    {%- set validation_config = var('edu:data_validation', {}) -%}

    {%- if validation_config.get('enabled', false) -%}
        ,
        cast(
            array() as {{ validation_struct_type() }}
        ) as validations,
        cast(null as string) as validation_severity

    {%- endif %}

{%- endmacro %}


{#
    Returns the validation result rows configured for a target model as a
    single UNION ALL query.

    Validators are registered under edu:data_validation.models, where each
    target model maps to the list of validator models whose findings should be
    applied to it.

    Each validator is expected to return the target model''s key columns plus a
    validation struct that follows the canonical validation contract.

    ref() is intentionally used for every configured validator so dbt records
    each validator as a dependency of the validation-merge node. This ensures
    all configured validators finish before their results are merged back into
    the target model.

    UNION ALL is used because each validator represents an independent finding.
    Multiple validators may produce findings for the same target row, and all
    of those findings must be preserved.

    If no validators are configured for the target model, this macro emits no
    SQL. Normally the corresponding validation-merge model will also be
    disabled in that case.
#}
{% macro validation_models_for(target_model) -%}

    {%- set validation_models =
        var('edu:data_validation', {}).get('models', {}).get(target_model, [])
    -%}

    {% if validation_models | length > 0 %}
        {% for validation_model in validation_models %}
            select *
            from {{ ref(validation_model) }}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    {% endif %}

{%- endmacro %}


{#
    Materializes validator results back into an edu_edfi_source staging model.

    Each configured validator produces one row per validation finding containing
    the target model''s key columns and a canonical validation struct.

    This materialization:
      - combines all validation findings for each target row into the validations
        array;
      - determines the row''s highest validation severity using the configured
        edu:data_validation.severities ranking; and
      - merges those values back into the already-built staging model without
        changing its grain or primary key.

    target_model identifies the staging model receiving the validation results.
    target_keys contains one or more columns that uniquely identify a row in
    that model and are used for both aggregation and the MERGE condition.

    When a severity ranking is configured, max_by() returns the severity having
    the highest configured rank. A severity not present in the configured
    ranking receives rank 0.

    If no severity ranking is configured, the framework cannot reliably
    determine which finding is most severe, so validation_severity is set to
    'unknown'.

    This replaces the staging model''s initially empty validation values. At this 
    point there are no previously propagated validations that need to be preserved.
#}
{% materialization edu_source_validation_merge -%}

    {# Identify the relation and key columns that will receive the results. #}
    {%- set target_model = config.require('target_model') -%}
    {%- set target_keys = config.require('target_keys') -%}
    {%- set target_relation = ref(target_model) -%}

    {# Severity rank determines which validation becomes the row-level severity. #}
    {%- set severities =
        var('edu:data_validation', {}).get('severities', {})
    -%}

    {%- call statement('main') %}

        merge into {{ target_relation }} as target

        using (
            select
                {# Preserve the complete target key, including composite keys. #}
                {% for key in target_keys %}
                    {{ adapter.quote(key) }}{% if not loop.last %},{% endif %}
                {% endfor %},

                {# Collect every finding for the target row into one array. #}
                collect_list(validation) as validations,

                {#
                    Determine the most severe finding according to the configured
                    severity ranking. Unknown/unranked severities receive rank 0.
                #}
                {% if severities | length > 0 %}
                    max_by(
                        validation.severity,
                        case validation.severity
                            {% for severity, rank in severities.items() %}
                            when '{{ severity }}' then {{ rank }}
                            {% endfor %}
                            else 0
                        end
                    )
                {% else %}
                    'unknown'
                {% endif %} as validation_severity

            from (
                {#
                    sql contains the UNION ALL of validator models generated by
                    validation_models_for().
                #}
                {{ sql }}
            ) as validation_rows

            group by
                {% for key in target_keys %}
                    {{ adapter.quote(key) }}{% if not loop.last %},{% endif %}
                {% endfor %}
        ) as source

        {# Match validation results back to the exact staging row. #}
        on
        {% for key in target_keys %}
            target.{{ adapter.quote(key) }} = source.{{ adapter.quote(key) }}{% if not loop.last %} and {% endif %}
        {% endfor %}

        {#
            The staging model begins with empty validation columns, so its
            validation state can be replaced directly with the aggregated
            results for the current run.
        #}
        when matched then update set
            target.validations = source.validations,
            target.validation_severity = source.validation_severity

    {% endcall -%}

    {#
        This materialization mutates target_model rather than creating a
        relation for the val_* dbt node itself.
    #}
    {{ return({'relations': []}) }}

{%- endmaterialization %}


{#
    Configures a validation-merge model for a target model and returns the SQL
    for all validators registered to that target.

    This macro is the common entry point used by val_* models. It keeps those
    models intentionally small by centralizing three responsibilities:

      - establishing a dbt dependency on the target model;
      - configuring the custom materialization that will apply the validation
        results; and
      - retrieving and combining all validator models registered to the target.

    materialization identifies the custom materialization responsible for
    applying the results. This allows the same wrapper to support different
    merge behavior for source/staging models and warehouse models.

    target_model identifies the model whose rows will receive the validation
    results.

    target_keys contains one or more columns that uniquely identify a target
    row and are used by the selected materialization when merging results.

    The explicit ref() to target_model is intentionally discarded. Its purpose
    is to establish the target as an upstream dbt dependency so the target is
    completely built before any validation results are applied to it.

    validation_models_for() supplies the actual model SQL by UNIONing every
    validator registered to target_model under edu:data_validation.models.

    Together, these dependencies ensure the execution order is:

        target model
            -> validator models
            -> validation merge

    The SQL comment emitted below has no effect on dbt dependency resolution;
    it exists only to make the otherwise invisible target dependency apparent
    when inspecting compiled SQL.
#}
{% macro apply_validations(materialization, target_model, target_keys) -%}

    {# Establish an upstream dependency without emitting the relation into SQL. #}
    {%- do ref(target_model) -%}

    /* validation: depends on {{ target_model }} */

    {{ config(
        materialized=materialization,
        target_model=target_model,
        target_keys=target_keys
    ) }}

    {{ edu_edfi_source.validation_models_for(target_model) }}

{%- endmacro %}


{#
    Returns the model to which a validator''s findings are applied.

    Validators are registered under edu:data_validation.models, where each key
    identifies a target model and its value contains the validators whose
    findings should be attached to that model.

    A validator may reference multiple models while evaluating a business rule.
    The value returned here does not identify every model used by the validator;
    it identifies the model whose record receives the resulting validation.

    This lookup is used when include_validator_application is enabled to
    populate the optional applies_to field in the validation struct.

    If the validator is not registered to any target, a descriptive fallback
    value is returned rather than failing compilation. This allows validation
    processing to continue while making the missing registration visible in
    the resulting validation metadata.
#}
{% macro validation_target(validator) -%}

    {%- set validation_config = var('edu:data_validation', {}) -%}
    {%- set validation_models = validation_config.get('models', {}) -%}

    {# Search the configured target -> validators mappings for this validator. #}
    {%- for target, validators in validation_models.items() -%}
        {%- if validator in validators -%}
            {{ return(target) }}
        {%- endif -%}
    {%- endfor -%}

    {# Preserve the validation while clearly identifying missing registration. #}
    {{ return('Validation target not found') }}

{%- endmacro %}

{#
    Builds a single validation struct using the canonical validation contract.

    Every validator must provide:
      - severity: relative importance of the validation finding
      - code: stable, machine-readable identifier for the validation rule
      - message: human-readable description of the specific finding

    These arguments are SQL expressions rather than literal values. This allows
    validators to supply either constants or values derived from the data being
    validated.

    Projects may extend the validation payload through
    edu:data_validation.additional_fields. Values for those fields can be passed
    through additional_values. If a configured additional field is not supplied
    by a validator, a typed null is emitted so every validation retains the same
    struct schema.

    Values supplied in additional_values that are not defined in
    additional_fields are intentionally ignored. The configured schema remains
    authoritative and individual validators cannot accidentally produce
    incompatible validation structs.

    Optional framework-managed provenance can also be included:
      - validator: name of the dbt model that generated the finding
      - applies_to: model whose record receives the finding

    applies_to is derived from edu:data_validation.models. A validator may use
    multiple models to evaluate its rule; applies_to identifies only the model
    to which the resulting validation is attached.
#}
{% macro validation_struct(severity, code, message, additional_values={}) -%}

    {%- set validation_config = var('edu:data_validation', {}) -%}
    {%- set additional_fields = validation_config.get('additional_fields', {}) -%}

    {%- set include_validator_name =
        validation_config.get('include_validator_name', false)
    -%}

    {%- set include_validator_application =
        validation_config.get('include_validator_application', false)
    -%}

    named_struct(
        {# Required validation contract. #}
        'severity', {{ severity }},
        'code', {{ code }},
        'message', {{ message }}

        {#
            Append implementation-defined fields. Missing values are emitted as
            typed nulls to preserve an identical struct schema across validators.
        #}
        {%- for field, data_type in additional_fields.items() %}
        ,'{{ field }}',
            {%- if field in additional_values %}
            {{ additional_values[field] }}
            {%- else %}
            cast(null as {{ data_type }})
            {%- endif %}
        {%- endfor %}

        {# Optionally record the dbt model responsible for the finding. #}
        {%- if include_validator_name %}
        ,'validator', '{{ model.name }}'
        {%- endif %}

        {# Optionally record the model to which this finding is attached. #}
        {%- if include_validator_application %}
            {%- set target = edu_edfi_source.validation_target(model.name) -%}
        ,'applies_to', '{{ target }}'
        {%- endif %}
    )

{%- endmacro %}