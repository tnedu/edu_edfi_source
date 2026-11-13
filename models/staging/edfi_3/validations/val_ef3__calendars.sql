{{ edu_edfi_source.apply_validations(
    materialization='edu_source_validation_merge',
    target_model='stg_ef3__calendars',
    target_keys=['k_school_calendar']
) }}