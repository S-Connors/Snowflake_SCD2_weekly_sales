
{% snapshot customer_snap %}

    {{
        config (
            target_schema = 'intermediate',
            strategy = 'check',
            unique_key = 'c_customer_sk',
            check_cols = 'all'
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
        C_SALUTATION
    from {{ source('ab_raw', 'customer')}}

{% endsnapshot %}