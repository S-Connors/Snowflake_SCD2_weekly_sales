{{
    config(
        materialized = 'incremental',
        schema = 'intermediate',
        unique_key = ['item_sk', 'warehouse_sk', 'sold_date_sk'],
        incremental_strategy = 'merge'
    )
}}

-- get max sold date from last data run
{% if is_incremental() %}
    {% set max_sold_day_query %}
        select ifnull(max(sold_date_sk), 0) from {{this}} as max_sold_day
    {% endset %}

    {% if execute %}ﬁ
        {% set max_sold_day = run_query(max_sold_day_query).columns[0][0] %}
    {% endif %}
{% endif %}

-- set latest_daily_date
-- get the date from date dimension 
{% set get_updated_daily_query %}
    select min(d_date_sk) from {{ source('ab_raw', 'date_dim') }}
        {% if is_incremental() %}
            where d_date_sk = '{{max_sold_day}}'
        {% endif %}
{% endset %}

{% if execute %}
    {% set latest_daily_date = run_query(get_updated_daily_query).columns[0][0] %}
{% endif %}


--Join catalog sales and web sales together
with all_sales as(
    select
        cs_sold_date_sk as sold_date_sk,
        cs_warehouse_sk as warehouse_sk,
        cs_item_sk as item_sk,
        cs_quantity as quantity,
        cs_net_profit as net_profit,
        cs_sales_price * cs_quantity as sales_amt
    from {{ source('ab_raw', 'catalog_sales') }}
-- this step filters the incremntal part and tracks where the new data starts
    where sold_date_sk >= '{{ latest_daily_date }}'

    union

    SELECT 
        WS_WAREHOUSE_SK as warehouse_sk,
        WS_ITEM_SK as item_sk,
        WS_SOLD_DATE_SK as sold_date_sk,
        WS_QUANTITY as quantity,
        ws_sales_price * ws_quantity as sales_amt,
        WS_NET_PROFIT as net_profit
    from {{ source('ab_raw', 'web_sales') }}
-- this step filters the incremntal part and tracks where the new data starts
    where sold_date_sk >= '{{ latest_daily_date }}'
),

-- join the date dimension to all sales
    daily_sales as (
        select 
            sold_date_sk, 
            warehouse_sk,
            item_sk,
            mnth_num,
            yr_num,
            yr_wk_num,
            cal_dt,
            sum(quantity) as daily_qty,
            sum(net_profit) as daily_profit,
            sum(sales_amt) as daily_sales_amt
        from all_sales
        left join {{ source('ab_raw','date_dim') }} on sold_date_sk = d_date_sk
        group by sold_date_sk, warehouse_sk, item_sk, mnth_num, yr_num, yr_wk_num, cal_dt
        
    )

select *
from daily_sales
where warehouse_sk is not null

