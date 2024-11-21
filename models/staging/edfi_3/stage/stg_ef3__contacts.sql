with base_contacts as (
    select * from {{ ref('base_ef3__contacts') }}
    where not is_deleted
),
base_parents as (
    select tenant_code, api_year, pull_timestamp, file_row_number, last_modified_timestamp, filename, 
        is_deleted, record_guid, ods_version, data_model_version, 
        parent_unique_id as contact_unique_id, 
        person_id, first_name, middle_name, last_name, maiden_name, generation_code_suffix, 
        personal_title_prefix, gender_identity, preferred_first_name, preferred_last_name, login_id, 
        sex, highest_completed_level_of_education, person_source_system, person_reference, v_addresses, 
        v_international_addresses, v_electronic_mails, v_telephones, v_languages, v_other_names, 
        v_personal_identification_documents, v_ext
    from {{ ref('base_ef3__parents') }}
    where not is_deleted
),
-- parents were renamed to contacts in Data Standard v5.0
unioned as (
    select * from base_contacts
    union 
    select * from base_parents
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(contact_unique_id)'
            ]
        ) }} as k_contact,
        unioned.*
        {{ extract_extension(model_name=[this.name, 'stg_ef3__parents'], flatten=True) }}
    from unioned
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_contact', 
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
