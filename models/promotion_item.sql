{{
    config(
        materialized = 'incremental',
        schema = 'analytics',
        unique_key = ['i_item_sk', 'p_promo_sk','p_start_date_sk'],
        incremental_strategy = 'merge'
    )
}}

-- get max start date from last data run
{% if is_incremental() %}
    {% set max_start_date_query %}
        select ifnull(max(P_START_DATE_SK), 0) from {{this}} as max_start_date
    {% endset %}

    {% if execute %}
        {% set max_start_date = run_query(max_start_date_query).columns[0][0] %}
    {% endif %}
{% endif %}

-- set latest_start_date
-- get the date from date dimension 
{% set get_updated_start_query %}
    select min(d_date_sk) from {{ source('ab_raw', 'date_dim') }}
        {% if is_incremental() %}
            where d_date_sk = '{{ max_start_date }}'
        {% endif %}
{% endset %}

{% if execute %}
    {% set latest_start_date = run_query(get_updated_start_query).columns[0][0] %}
{% endif %}

select
    P_CHANNEL_CATALOG,
    P_CHANNEL_DEMO,
    P_CHANNEL_DETAILS,
    P_CHANNEL_DMAIL,
    P_CHANNEL_EMAIL,
    P_CHANNEL_EVENT,
    P_CHANNEL_PRESS,
    P_CHANNEL_RADIO,
    P_CHANNEL_TV,
    P_COST,
    P_DISCOUNT_ACTIVE,
    P_END_DATE_SK,
    P_PROMO_ID as promo_id,
    P_PROMO_NAME as promo_name,
    P_PROMO_SK as promo_sk,
    P_PURPOSE,
    P_RESPONSE_TARGET,
    P_START_DATE_SK,
    I_ITEM_SK as item_sk,
    I_BRAND,
    I_BRAND_ID,
    I_CATEGORY,
    I_CATEGORY_ID,
    I_CLASS,
    I_CLASS_ID,
    I_COLOR,
    I_CONTAINER,
    I_CURRENT_PRICE,
    I_FORMULATION,
    I_ITEM_DESC,
    I_ITEM_ID,
    I_MANAGER_ID,
    I_MANUFACT,
    I_MANUFACT_ID,
    I_PRODUCT_NAME,
    I_REC_END_DATE,
    I_REC_START_DATE,
    I_SIZE,
    I_UNITS,
    I_WHOLESALE_COST
from {{ source('ab_raw','promotion') }}
join {{ source('ab_raw', 'item') }} on p_item_sk = i_item_sk
where p_start_date_sk >= '{{ latest_start_date }}' and p_start_date_sk is not null


