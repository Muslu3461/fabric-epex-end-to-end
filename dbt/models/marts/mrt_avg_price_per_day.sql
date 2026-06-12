with stg as (
    select *
    from {{ ref('stg_epex_spot_prices') }}
),

final as (
    select
        market,
        country,
        region,
        date,
        avg(price_cent_kwh) as avg_day_price
    from stg
    group by market, country, region, date
)

select * from final
