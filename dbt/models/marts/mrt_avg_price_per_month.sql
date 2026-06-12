with stg as (
    select *
    from {{ ref('stg_epex_spot_prices') }}
),

with_month as (
    select
        *,
        datetrunc(month, date)  as month,
        year(date)              as year,
        format(date, 'MMM')     as month_name_short
    from stg
),

final as (
    select
        market,
        country,
        region,
        month,
        month_name_short,
        year,
        avg(price_cent_kwh) as avg_month_price
    from with_month
    group by market, country, region, month, month_name_short, year
)

select * from final
