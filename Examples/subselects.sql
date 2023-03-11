select  ng.year, nonGXD, GXD, nonGXD + GXD as total
from
(
select  b.year, count(distinct b._refs_key) as nonGXD
from bib_refs b left join gxd_index i on (b._refs_key = i._refs_key)
where i._refs_key is NULL
and b.year in (2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
group by b.year
) as ng

join 

(select  b.year , count(distinct b._refs_key) as GXD
from bib_refs b join gxd_index i on (b._refs_key = i._refs_key)
where b.year in (2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
group by b.year
) as g

on (ng.year = g.year)
