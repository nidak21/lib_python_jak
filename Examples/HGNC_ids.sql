-- find human markers with no HGNC ID but are in a HomoloGene cluster with
--   a mouse gene.
create temporary table tmp_markers
as
select m.symbol, m._marker_key
from mrk_marker m join acc_accession a on (m._marker_key = a._object_key and a._mgitype_key = 2 and m._organism_key = 2 and a._logicaldb_key in (64, 55))
group by m.symbol, m._marker_key
order by m._marker_key
||
select count(*) from tmp_markers
||
create index idx1 on tmp_markers(_marker_key)
|| 
create temporary table tmp_markers2
as
select m.symbol, m._marker_key, count(distinct a.accid) as HGNC_count
from tmp_markers m left join acc_accession a on (m._marker_key = a._object_key and
a._mgitype_key = 2 and  a._logicaldb_key = 64)
group by m.symbol, m._marker_key
|| 
select count(*) from tmp_markers2
||
create index idx2 on tmp_markers2(_marker_key)
|| 
create temporary table tmp_markers3
as 
select m.symbol, m._marker_key, m.HGNC_count, c._cluster_key, c._clustersource_key
from tmp_markers2 m left join  
    ( mrk_clustermember cm  join mrk_cluster c on (cm._cluster_key = c._cluster_key and c._clustersource_key = 9272151) )  on (m._marker_key = cm._marker_key) 
||
select count(*) from tmp_markers3
||
create index idx3 on tmp_markers3(_cluster_key)
||
select m.symbol, m._marker_key, m.HGNC_count, cm._cluster_key, mm.symbol
from tmp_markers3 m left join 
      ( mrk_clustermember cm join mrk_marker mm 
         on (cm._marker_key = mm._marker_key and mm._organism_key = 1) )
      on  (m._cluster_key = cm._cluster_key)
where m.HGNC_count = 0
and    cm._cluster_key is not Null
limit 100

