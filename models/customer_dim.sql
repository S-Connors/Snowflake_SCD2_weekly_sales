{{

    config(
        materialized = 'table',
        schema = 'analytics'
    )
}}


select 
    C_BIRTH_COUNTRY,
    C_BIRTH_DAY,
    C_BIRTH_MONTH,
    C_BIRTH_YEAR,
    C_CURRENT_ADDR_SK,
    C_CURRENT_CDEMO_SK,
    C_CURRENT_HDEMO_SK,
    C_CUSTOMER_ID,
    C_CUSTOMER_SK,
    C_EMAIL_ADDRESS,
    C_FIRST_NAME,
    C_FIRST_SALES_DATE_SK,
    C_FIRST_SHIPTO_DATE_SK,
    C_LAST_NAME,
    C_LAST_REVIEW_DATE_SK,
    C_LOGIN,
    C_PREFERRED_CUST_FLAG,
    C_SALUTATION,
    CA_ADDRESS_ID,
    CA_ADDRESS_SK,
    CA_CITY,
    CA_COUNTRY,
    CA_COUNTY,
    CA_GMT_OFFSET,
    CA_LOCATION_TYPE,
    CA_STATE,
    CA_STREET_NAME,
    CA_STREET_NUMBER,
    CA_STREET_TYPE,
    CA_SUITE_NUMBER,
    CA_ZIP,
    CD_DEMO_SK,
    CD_DEP_COLLEGE_COUNT,
    CD_DEP_COUNT,
    CD_DEP_EMPLOYED_COUNT,
    CD_EDUCATION_STATUS,
    CD_GENDER,
    CD_MARITAL_STATUS,
    CD_PURCHASE_ESTIMATE,
    HD_BUY_POTENTIAL,
    HD_DEMO_SK,
    HD_DEP_COUNT,
    HD_INCOME_BAND_SK,
    HD_VEHICLE_COUNT,
    IB_INCOME_BAND_SK,
    IB_LOWER_BOUND,
    IB_UPPER_BOUND,
    DBT_VALID_FROM as valid_from,
    DBT_VALID_TO as valid_to

from {{ ref('customer_snap') }}
join {{ source('ab_raw', 'customer_address') }} on C_CURRENT_ADDR_SK = ca_address_sk
join {{ source('ab_raw', 'customer_demographics') }} on C_CURRENT_CDEMO_SK = CD_DEMO_SK
join {{ source('ab_raw', 'household_demographics') }} on C_CURRENT_HDEMO_SK = hd_demo_sk
join {{ source('ab_raw', 'income_band') }} on HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
where valid_to is null