select 
url_extract_host(link),
trim(lower(place)), 
count(*) ,
approx_percentile(round(cast(replace(price, ' ') as double) / cast(replace(area, ' ') as double)), 0.5) as median 
from real_estate_db.daily_measurements 
where url_extract_host(link) not in ('etuovi.com', 'www.vuokraovi.com', 'sofia.holmes.bg')
and try_cast(replace(price, ' ') as double) is not null
and try_cast(replace(area, ' ') as double) is not null 
group by 1, 2
order by 4 desc