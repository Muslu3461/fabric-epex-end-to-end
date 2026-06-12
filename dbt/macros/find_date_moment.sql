{% macro find_date_moment(which_moment) %}
    {% set order = "asc" %}
    {% if which_moment == "highest" %}
        {% set order = "desc" %}
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
                order by price_cent_kwh {{ order }}
            ) as rn
        from stg
    ),

    calc_result as (
        select
            date,
            market,
            start_time,
            end_time,
            price_cent_kwh,
            rn
        from with_rank
        where rn = 1
    )
{% endmacro %}
