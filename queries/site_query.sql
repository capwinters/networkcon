select SiteName
, avg(Latitude) as lat
, avg(Longitude) as lon
, 0.5*cast(avg(CellParam2) as integer) as site_kpi
, cast(avg(CellParam3) as integer) as site_parameter
from network a
group by SiteName
limit 100