with stage_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        ed_org_id,
        k_lea,
        k_school,
        {{ extract_descriptor('value:studentIdentificationSystemDescriptor::string') }} as id_system,
        value:assigningOrganizationIdentificationCode::string as assigning_org,
        value:identificationCode::string as id_code
    from stage_stu_ed_org
        , lateral variant_explode(v_student_identification_codes)
)
select * from flattened