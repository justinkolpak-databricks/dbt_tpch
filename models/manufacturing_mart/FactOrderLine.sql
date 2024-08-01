{{ config(materialized='table') }}

select CONCAT(sli.l_orderkey || '|' || sli.l_linenumber) AS order_line_item_id
    , sli.l_orderkey AS order_id
    , sli.l_linenumber AS line_item_number
    , so.o_custkey AS sk_customer_id
    , sli.l_quantity AS quantity
    , sli.l_extendedprice AS extended_price
    , sli.l_discount AS discount_pct
    , sli.l_tax AS tax
    , CASE sli.l_returnflag 
        WHEN 'R' THEN true
        ELSE false
    END AS is_returned
    , CASE sli.l_linestatus 
        WHEN 'O' THEN 'Open'
        WHEN 'F' THEN 'Fulfilled'
    END AS line_item_status
    , so.o_orderstatus AS order_status
    , so.o_order_date AS order_date
    , so.o_orderpriority AS order_priority
    , sli.l_shipdate AS ship_date
    , sli.l_commitdate AS commit_date
    , sli.l_receiptdate AS receipt_date
    , sli.l_shipmode AS shipment_mode
    , sli.l_partkey AS sk_part_id
    , sli.l_suppkey AS sk_supplier_id
FROM {{ source('tpch', 'silver_lineitem') }} sli
LEFT JOIN {{ source('tpch', 'silver_orders') }} so