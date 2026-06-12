-- Gün içi fiyatları düşükten yükseğe sıralar, batarya kullanım tavsiyesi üretir
{% set markets_query %}
    select distinct market
    from {{ ref('stg_epex_spot_prices') }}
    order by market
{% endset %}

{% set markets_query_results = run_query(markets_query) %}
{% if execute %}
    {% set markets = markets_query_results.columns[0].values() %}
{% else %}
    {% set markets = [] %}
{% endif %}

with stg as (
    select
        market,
        date,
        start_time,
        end_time,
        price_cent_kwh
    from {{ ref('stg_epex_spot_prices') }}
),

with_rank as (
    select
        *,
        row_number() over (
            partition by date, market
            order by price_cent_kwh asc
        ) as rn
    from stg
),

final as (
    select
        market,
        date,
        datepart(hour, start_time) as hour_of_day, 
        substring(convert(nvarchar, start_time, 14), 1, 5) as start_time,
        substring(convert(nvarchar, end_time,   14), 1, 5) as end_time,
        price_cent_kwh,
        case
            when price_cent_kwh < 0   then 'discharge'
            when rn < 10              then 'charge+grid'
            when rn < 18              then 'grid'
            when rn < 24              then 'battery'
            else                           'battery+discharge'
        end as simple_advice
    from with_rank
)

select * from final
