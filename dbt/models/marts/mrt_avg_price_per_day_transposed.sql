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

with avgs as (
    select *
    from {{ ref('mrt_avg_price_per_day') }}
),

final as (
    select
        date,
        {% for market in markets %}
        sum(
            case
                when market = '{{ market }}' then avg_day_price
                else 0
            end
        ) as market_{{ market | replace("-","_") | lower }}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from avgs
    group by date
)

select * from final
