{{
    config(
        materialized = 'incremental',
        schema = 'analytics',
        tabel_name = 'weekly_sales',
        unique_key = ['item_sk', 'warehouse_sk', 'yr_wk_num'],
        incremental_strategy = 'merge'
    )
}}

-- get max date from last data run
{% if is_incremental() %}
    {% set max_sold_wkly_query %}
        select ifnull(max(yr_wk_num), 0) from {{this}} as max_sold_wkly
    {% endset %}

    {% if execute %}
        {% set max_sold_wkly = run_query(max_sold_wkly_query).columns[0][0] %}
    {% endif %}
{% endif %}

{% set get_updated_wkly_query %}
    select min(d_date_sk) from {{ source('ab_raw', 'date_dim') }}
        {% if is_incremental() %}
            where d_date_sk =  '{{ max_sold_wkly }}'
        {% endif %}
{% endset %}

{% if execute %}
    {% set latest_wkly_date = run_query(get_updated_wkly_query).columns[0][0] %}
{% endif %}

select 
    warehouse_sk,
    item_sk,
    yr_num,
    mnth_num,
    yr_wk_num,
    sum(daily_qty) as sum_qty_wk,
    sum(daily_sales_amt) as sum_amt_wk,
    sum(daily_profit) as sum_profit_wk,
    sum(daily_qty)/7 as avg_qty_dy,
    sum(coalesce(inv_quantity_on_hand, 0)) as inv_qty_wk,
    sum(coalesce(inv_quantity_on_hand, 0))/sum(daily_qty) as wks_sply,
    iff(avg_qty_dy >0 and avg_qty_dy > inv_qty_wk, true, false) as low_stock_flg_wk
from {{ ref('daily_sales')}}
join {{ source('ab_raw', 'inventory')}} on inv_date_sk = sold_date_sk and item_sk = inv_item_sk and inv_warehouse_sk = warehouse_sk
where sold_date_sk >= '{{latest_wkly_date}}'
group by 1, 2, 3, 4, 5

#
