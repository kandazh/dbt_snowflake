-- TPC-DS_query9
select case when (select count(*) 
                  from {{ source('tpcds_sf10tcl', 'store_sales') }}  
                  where ss_quantity between 1 and 20) > 177560856
            then (select avg(ss_ext_discount_amt) 
                  from {{ source('tpcds_sf10tcl', 'store_sales') }}  
                  where ss_quantity between 1 and 20) 
            else (select avg(ss_net_paid_inc_tax)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 21 and 40) > 165030289
            then (select avg(ss_ext_discount_amt)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 21 and 40) 
            else (select avg(ss_net_paid_inc_tax)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 41 and 60) > 357290662
            then (select avg(ss_ext_discount_amt)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 41 and 60)
            else (select avg(ss_net_paid_inc_tax)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 61 and 80) > 205197751
            then (select avg(ss_ext_discount_amt)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 61 and 80)
            else (select avg(ss_net_paid_inc_tax)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 81 and 100) > 80111186
            then (select avg(ss_ext_discount_amt)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 81 and 100)
            else (select avg(ss_net_paid_inc_tax)
                  from {{ source('tpcds_sf10tcl', 'store_sales') }} 
                  where ss_quantity between 81 and 100) end bucket5
from {{ source('tpcds_sf10tcl', 'reason') }} 
where r_reason_sk = 1

