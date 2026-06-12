with src as (
    select *
    from {{ source('epex_spot_prices', 'epex_spot_prices') }}
),

conversions as (
    select
        market,
        convert(date, start_time)  as date,
        convert(time, start_time)  as start_time,
        convert(time, end_time)    as end_time,
        price / 10                 as price_cent_kwh
    from src
),

with_country as (
    select
        *,
        case
            when market like 'NO%'   then 'Norway'
            when market like 'SE%'   then 'Sweden'
            when market like 'DK%'   then 'Denmark'
            when market = 'DE-LU'    then 'Germany'
            when market = 'FI'       then 'Finland'
            when market = 'BE'       then 'Belgium'
            when market = 'PL'       then 'Poland'
            when market = 'AT'       then 'Austria'
            when market = 'FR'       then 'France'
            when market = 'NL'       then 'the Netherlands'
            when market = 'CH'       then 'Switzerland'
            when market = 'GB'       then 'United Kingdom'
            else 'Unknown'
        end as country
    from conversions
),

final as (
    select
        *,
        case
            when country in ('Belgium','the Netherlands','Germany','France','Switzerland','Austria')
                then 'West Europe'
            when country in ('United Kingdom')
                then 'North Europe'
            when country in ('Poland')
                then 'Central Europe'
            when country in ('Norway','Sweden','Finland','Denmark')
                then 'Scandinavia'
            else 'Unknown'
        end as region
    from with_country
    where price_cent_kwh > 0
)

select
    market,
    date,
    start_time,
    end_time,
    price_cent_kwh,
    country,
    region
from final
