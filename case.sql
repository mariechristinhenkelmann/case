/* README: this script contains 4 steps: staging & loading, transformation, datamarts and analysis view. written for snowflake.


/* STEP 1: STAGING & LOADING 

/* create database for staged data, fileformats etc. */
create or replace database manage_db;
create or replace schema internal_stage;

use database manage_db;
use schema internal_stage;

/* create internal stage */
create or replace stage fortune1000sales
    file_format = (type = csv);   
    
describe stage fortune1000sales;

/* upload data to stage: put file command over CLI SnowSQL */

list @fortune1000sales;

/* create database, schema, table for raw data */
create or replace database raw;
create or replace schema fortune1000;

use database raw;
use schema fortune1000;

create or replace table raw.fortune1000.sales (
    company_rank varchar(5),
    company_name varchar(50),
    revenue varchar, 
    revenue_percent_change varchar, 
    profit_million varchar,
    profit_percent_change varchar, 
    assets varchar, 
    market_value varchar, 
    rank_change varchar,
    employees varchar, 
    rank_change_500 varchar,
    measure_up_rank varchar,
    filename varchar(1000)
);

/* copy data from stage into table */
use database manage_db;
use schema internal_stage;

copy into raw.fortune1000.sales(
    company_rank,
    company_name,
    revenue, 
    revenue_percent_change, 
    profit_million,
    profit_percent_change, 
    assets, 
    market_value, 
    rank_change,
    employees, 
    rank_change_500,
    measure_up_rank,
    filename
    )
    from (select 
            s.$1, 
            s.$2, 
            s.$3, 
            s.$4, 
            s.$5, 
            s.$6, 
            s.$7, 
            s.$8, 
            s.$9, 
            s.$10, 
            s.$11, 
            s.$12, 
            metadata$filename 
          from @fortune1000sales s)
    file_format = ( 
        type = csv 
        field_delimiter = ';'
        skip_header = 1
        field_optionally_enclosed_by = '"'
        null_if = '-'
        error_on_column_count_mismatch = false 
        );
    -- validation_mode = return_errors;

-- select * from raw.fortune1000.sales limit 100;



/* STEP 2: TRANSFORMING */

/* create database, schema, table for transformed data */
create database transformed;
create schema analytics;

/* FROM RAW DATA */
/* transforming table => stg_fortune1000_sales */
create or replace view transformed.analytics.stg_fortune1000_sales 
as ( 
    select 
        company_rank,
        company_name,
        regexp_replace(revenue,'[$,]','')::int as revenue_usd,
        case 
            when contains(revenue_percent_change,'%') then regexp_replace(revenue_percent_change,'%','')::float/100 
            else round(regexp_replace(revenue_percent_change, ',','.'),2)::float
        end as revenue_percent_change,
        regexp_replace(profit_million,'[$,]','')::int as profit_million_usd,
        case 
            when contains(profit_percent_change,'%') then regexp_replace(profit_percent_change,'%','')::float/100 
            else round(regexp_replace(profit_percent_change, ',','.'),2)::float
        end as profit_percent_change, 
        regexp_replace(assets,'[$,]','')::int as assets_usd, 
        regexp_replace(market_value,'[$,]','')::int as market_value_usd, 
        regexp_replace(employees,'[^0-9]','')::int as employees, 
        right(regexp_replace(filename,'[.csv.gz]',''),4) as year
    from raw.fortune1000.sales
);

select * from transformed.analytics.stg_fortune1000_sales
limit 100;


/* FROM MARKETPLACE DATA */
/* rename marketplace data */
alter database gaialens_environmental_data_sample rename to marketplace_gaialens_esg;

/* transforming table => stg_fortune1000_esg */
create or replace view transformed.analytics.stg_fortune1000_esg
as (
    select
        /* in dbt, the following case statement could have been done by setting an array of values 
           and then using a macro/for loop that gets compiled to the following code, following DRY principles */
        case 
            when contains(lower("companyname"),' a/s') then initcap(regexp_replace(lower("companyname"),' a/s',''))
            when contains(lower("companyname"),' ag') then initcap(regexp_replace(lower("companyname"),' ag',''))
            when contains(lower("companyname"),' corporation') then initcap(regexp_replace(lower("companyname"),' corporation',''))
            when contains(lower("companyname"),' incorporated') then initcap(regexp_replace(lower("companyname"),' incorporated',''))
            when contains(lower("companyname"),', inc.') then initcap(regexp_replace(lower("companyname"),', inc.',''))
            when contains(lower("companyname"),',inc.') then initcap(regexp_replace(lower("companyname"),',inc.',''))
            when contains(lower("companyname"),' inc.') then initcap(regexp_replace(lower("companyname"),' inc.',''))
            when contains(lower("companyname"),', ltd.') then initcap(regexp_replace(lower("companyname"),', ltd.',''))
            when contains(lower("companyname"),',ltd.') then initcap(regexp_replace(lower("companyname"),',ltd.',''))
            when contains(lower("companyname"),' ltd.') then initcap(regexp_replace(lower("companyname"),' ltd.',''))
            when contains(lower("companyname"),', limited') then initcap(regexp_replace(lower("companyname"),', limited',''))
            when contains(lower("companyname"),',limited') then initcap(regexp_replace(lower("companyname"),',limited',''))
            when contains(lower("companyname"),' limited') then initcap(regexp_replace(lower("companyname"),' limited',''))
            when contains(lower("companyname"),' n.v.') then initcap(regexp_replace(lower("companyname"),' n.v.',''))
            when contains(lower("companyname"),' plc') then initcap(regexp_replace(lower("companyname"),' plc',''))
            when contains(lower("companyname"),' s.a.') then initcap(regexp_replace(lower("companyname"),' s.a.',''))
            when contains(lower("companyname"),' se') then initcap(regexp_replace(lower("companyname"),' se',''))
        else initcap("companyname")
        end as company_name,
        "country" as country,
        "sector" as sector,
        "fiscalyear"::text as year,
        "ghg_emissions"::int as ghg_emissions,
        "environmental_impacts"::int as environmental_impact
    from marketplace_gaialens_esg.scores.scores_e_pillar_sample
);
 
-- select * from transformed.analytics.stg_fortune1000_esg limit 100;



/*
/* STEP 3: BUILDING A DATAMARTS (not implemented in this exersice, but could look like the following)
/* list of some dims and fcts to be build, with an example code snippet for dim_company
/* the proper implementation might impact the organisation of schema in the transformed database 

    - dim_company
    
            with unioned_sources (
                select copmany_name from stg_fortune1000_sales
                union 
                select company_name from stg_fortune1000_esg ),
            
            , all_distinct_companies (
                select distinct company_name from unioned_sources
            )
            
            select 
                md5(cast(coalesce(cast(company_name as text),'') as text)) as company_id,
                company_name 
            from all_distinct_companies;
            
    - dim_sectors
    - dim_country
    - ...
    - fct_revenue
    - fct_emissions
    - ...
*/



/* STEP 4: CREATING CUSTOM VIEW FOR ANALYSIS/REPORTING */
create or replace view transformed.analytics.reporting_fortune1000
as (
    with sales as (
        select * from transformed.analytics.stg_fortune1000_sales
    )

    , esg as (
        select * from transformed.analytics.stg_fortune1000_esg
    )

    select 
        s.company_name, 
        s.company_rank,
        s.year,
        s.revenue_usd, 
        g.ghg_emissions, 
        g.environmental_impact
    from sales as s 
    left join esg as g 
        on upper(s.company_name) = upper(g.company_name)
        and s.year = g.year
    where g.company_name is not null
);

-- select * from transformed.analytics.reporting_fortune1000 limit 100;