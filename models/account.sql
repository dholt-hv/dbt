with a as (select account_key 
  , id 
  , envid 
  , name 
  , salesforce_id
  , left(salesforce_id, 15) salesforce_account_id
  , is_sales_demo_source_account 
  , case when preferred_grading_scale >= 3 and preferred_grading_scale <= 5 then preferred_grading_scale else 0 end preferred_grading_scale
from allraw_prd.hvnext_account a 
where salesforce_id is not null and is_sales_demo_source_account = 'false')

, pg as (select lookup_type 
  , lookup_value 
  , lookup_name
from allraw_prd.e_lookups
where lookup_type = 'position_grading_scale')

select a.account_key 
  , a.id as account_id
  , a.envid as environment_id
  , a.name as customer_name
  , coalesce(sa.unique_id_15_char_c, a.salesforce_id) as salesforce_account_id
  , sa.id as salesfroce_18_char_account_id 
  , sa.type as salesforce_type 
  , sa.territory_c as salesforce_region 
  , pg.lookup_name as position_grading_scale 
  , sa.account_temperature_2_c as account_temperature 
  , sa.first_closed_opportunity_date_c as first_opportunity_closed_date 
  , sa.hv_industry_classification_c as industry_classification 
  , sa.sic_description_c as sic_industry_code 
  , sa.sic_description_c as sic_industry_description
  , sa.number_of_employees as employee_count 
  , sa.number_of_employees_c as employee_count_bucketed 
  , sa.billing_city 
  , sa.billing_state 
  , sa.billing_country
  , sa.billing_postal_code 
  , su.name as custom_success_manager_name 
  , sa.cs_team_c as customer_success_team 
  , sa.name_use_c as customer_name_use 
  , sa.active_mrr_c * 12 as active_arr 
from a 
left join salesforce.accounts sa on sa.unique_id_15_char_c = a.salesforce_account_id
left join salesforce.users su on su.id = sa.account_manager_c
left join pg on pg.lookup_value = a.preferred_grading_scale