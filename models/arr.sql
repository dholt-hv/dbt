with initial_cte as (select op.opportunity_id 
	, op.product_name_c 
	, op.product_family_c as product_family
	, op.sbcf_acv_year_reporting_c as acv_yr
	, min(op.sbcf_acv_year_reporting_c) over (partition by op.opportunity_id) as min_acv
	, convert(float, op.total_price) /  dcr.conversion_rate as total_price
	, convert(float, op.total_price) as original_total
	, convert(float, o.sbcf_at_bat_total_c) / dcr.conversion_rate as at_bat_total
	, op.id as opportunity_product_id 
	, op.sbcf_quote_line_quantity_c  
	, op.quantity 
	, op.sbcf_subscription_category_c 
	, op.currency_iso_code 
	, op.subscription_length_c 
	, op.stage_c 
	, op.is_deleted 
	, o.account_id 
	, o.id_18_digit_c 
	, o.name 
	, o.close_date  
	, o.sbcf_subscription_end_date_c 
	, o.sbcf_subscription_start_date_c  
	, o.is_deleted 
	, a.name as salesforce_account_name
	, a.parent_account_id_18_c 
	, a.account_temperature_2_c 
	, a.hv_industry_classification_c 
	, a.billing_city 
	, a.billing_state 
	, a.billing_country 
	, a.type 
	, a.first_closed_opportunity_date_c
	, dcr.conversion_rate 
	, dcr.start_date 
	, dcr.next_start_date  
	, dcr.iso_code 
from salesforce.opportunity_product op 
inner join salesforce.opportunities o on o.id = op.opportunity_id 
inner join salesforce.accounts a on a.id = o.account_id
left join salesforce.dated_conversion_rate dcr on dcr.iso_code = op.currency_iso_code  and o.close_date >= dcr.start_date and o.close_date < dcr.next_start_date
where op.stage_c = 'Closed Won' and op.is_deleted  = 'false' and product_name_c not like '%Essentials Implementation%')
, arr_fix as (
select *
	, case when min_acv > 1 then acv_yr - min_acv + 1 else acv_yr end acv_yr_calculated
from initial_cte)
, month_year as (
select distinct date_trunc('month', date)::date as month_year
from helper_tables.calendar c)
, start_end_date as (
select dateadd('year', (acv_yr_calculated::integer-1), sbcf_subscription_start_date_c) as year_start_date
	, case
		when datediff('month', sbcf_subscription_start_date_c,sbcf_subscription_end_date_c) <=12 then sbcf_subscription_end_date_c
		when dateadd('day', -1, dateadd('year', (acv_yr_calculated::integer-1), sbcf_subscription_start_date_c))>= sbcf_subscription_end_date_c then sbcf_subscription_end_date_c
		when sbcf_subscription_end_date_c > dateadd('day', -1, dateadd('year', (acv_yr_calculated::integer-1), sbcf_subscription_start_date_c)) then dateadd('day', -1, dateadd('year', (acv_yr_calculated::integer), sbcf_subscription_start_date_c))
		end year_end_date_draft
	, coalesce(sbcf_quote_line_quantity_c, quantity) as quantity 
	, *
from arr_fix)
, year_fix as (
select *
	, case when year_end_date_draft > sbcf_subscription_end_date_c then sbcf_subscription_end_date_c else year_end_date_draft end year_end_date
from start_end_date
)
, months as (
select 
	 case when datediff('month', sbcf_subscription_start_date_c,sbcf_subscription_end_date_c)>12 then datediff('month', year_start_date, year_end_date)
    	else datediff('month', sbcf_subscription_start_date_c,dateadd('day', 1,sbcf_subscription_end_date_c))
		end months
	, datediff('month', sbcf_subscription_start_date_c,sbcf_subscription_end_date_c)
	, datediff('month', year_start_date, year_end_date)
	, dateadd('day', 1,sbcf_subscription_end_date_c)
	, year_end_date - year_start_date
	, *
from year_fix)
, months_adjusted as ( 
select case when months = 11 then 12 else months end number_of_months 
	, case when months  = 0 then total_price::float 
    	else total_price/(case when months = 11 then 12 else months end) end mrr
	, *
from months)
, month_year_join as (
select my.*
	 , case when product_name_c like '%Assessments%' then 'assessments'
		   when product_name_c = 'Custom Game-Based' then 'assessments'
		   when product_name_c = 'HireVue Bundle' then 'bundle'
		   when product_name_c like '%Coach%' then 'coach'
		   when product_name_c = 'HireVue CodeVue' then 'codevue'
		   when product_name_c like '%Coordinate%' then 'coordinate'
		   when product_name_c like '%Video Interviewing%' then 'video interviewing'
		   when product_name_c = 'HireVue Read Only Access Subscription' then 'read only'
		   when product_name_c like '%HireVue Hiring Assistant%' then 'HHA'
		   when product_name_c in ('HireVue Direct Messaging', 'HireVue Job Search Assistant', 'HireVue Recruiting Assistant', 'AllyO by HireVue Placeholder', 'HireVue Scheduling Assistant', 'AllyO Video Interviewing') then 'HHA'
		   when product_name_c like '%Builder%' then 'builder'
		   when product_name_c = 'Miscellaneous - Subscription' then 'builder'
		  else null
		end product_name
	, case when sbcf_subscription_category_c = 'Hires' then 'hires'
		   when sbcf_subscription_category_c = 'Employees' then 'employees'
		   when sbcf_subscription_category_c = 'Govt FTE' then 'employees'
		   when sbcf_subscription_category_c = 'Assessments' then 'assessments'
		   when sbcf_subscription_category_c = 'Pre-Built Model' then 'model_count'
		   when sbcf_subscription_category_c = 'Custom-Built Model' then 'model_count'
		   when sbcf_subscription_category_c = 'Job Families' then 'model_count'
		   when sbcf_subscription_category_c = 'Seats' then 'users'
		   when sbcf_subscription_category_c = 'Staffing Recruiters' then 'users'
		   when sbcf_subscription_category_c = 'Users' then 'users'
		  else null
		 end subscription_category
	, m.*
from months_adjusted m
right join month_year my on date_trunc('month',m.year_start_date) <= my.month_year and date_trunc('month', dateadd('day', 1, m.year_end_date)) > my.month_year
)
, dupe_destroyer as (select distinct month_year
	, salesforce_account_name as customer_name
	, sbcf_subscription_end_date_c
	, parent_account_id_18_c
	, hv_industry_classification_c as industry_classification
	, type as salesforce_type
	, name as opportunity_name
	, opportunity_id 
	, product_name
	, product_name_c as product_name_detailed
	, subscription_category
	, year_start_date
	, year_end_date
	, min_acv
	, acv_yr
	, acv_yr_calculated
	, case when number_of_months = 0 then number_of_months + 1 else number_of_months end number_of_months
	, conversion_rate
	, mrr 
	, total_price as arr
	, at_bat_total
from month_year_join myj
where product_name not in ('coach', 'bundle', 'read only', 'professional services'))
select month_year
	, customer_name
	, parent_account_id_18_c
	, industry_classification
	, salesforce_type
	, opportunity_name
	, opportunity_id 
	, product_name
	, product_name_detailed
	, subscription_category
	, year_start_date
	, year_end_date
	, acv_yr_calculated as acv_yr
	, datediff('days', year_start_date, year_end_date) as number_of_days
	, datediff('days', year_start_date, year_end_date) * 1.0 / 30 as number_of_months
	, round(datediff('days', year_start_date, year_end_date) * 1.0 / 30) number_of_months_round
	, conversion_rate
	, sum(mrr) as mrr
	, sum(arr) as orig_arr
	, round(sum(arr) / (datediff('days', year_start_date, year_end_date) / 30) * 12) as arr
from dupe_destroyer
where datediff('days', year_start_date, year_end_date) <> 0 and (datediff('days', year_start_date, year_end_date) / 30) <> 0 and ((mrr <> 0) or (product_name = 'coordinate'))
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
order by 1, 2 asc
 

