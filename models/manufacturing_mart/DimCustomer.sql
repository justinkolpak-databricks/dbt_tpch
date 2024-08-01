{{ config(materialized='table') }}

SELECT sc.c_custkey AS sk_customer_id
    , sc.c_name as full_name
    , sc.c_address as address_line_1
    , sc.c_phone as phone_number
    , sc.c_acctbal as account_balance
    , sc.c_mktsegment as market_segment
    , sc.c_comment as comment
    , sn.n_name as nation_name
    , sr.r_name as region_name
FROM {{ source('tpch', 'silver_customer') }} sc
LEFT JOIN {{ source('tpch', 'silver_nation') }} sn ON sc.c_nationkey = sn.n_nationkey
LEFT JOIN {{ source('tpch', 'silver_region') }} sr ON sn.n_regionkey = sr.r_regionkey;