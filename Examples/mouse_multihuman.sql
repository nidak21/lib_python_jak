-- find all mouse genes with >1 human homologs in HomoloGene or HGNC
-- 9272151 = homologene homology source
-- 13437099 = HGNC homology source
-- 13764519 = hybrid
create temporary table tmp_marker
as
select m.symbol, m._marker_key, c._cluster_key, t.term source
from mrk_marker m join
    ( mrk_clustermember cm join mrk_cluster c
      on (cm._cluster_key = c._cluster_key
	    and c._clustersource_key in (9272151, 13437099, 13764519) )
	join voc_term t on (c._clustersource_key = t._term_key)
    )
   on (m._marker_key = cm._marker_key and m._organism_key = 1)

||
select m.symbol, m._marker_key,  m.source, count(distinct mm._marker_key) numHuman
from tmp_marker m  join 
      ( mrk_clustermember cm join mrk_marker mm 
         on (cm._marker_key = mm._marker_key and mm._organism_key = 2) )
      on  (m._cluster_key = cm._cluster_key)
group by m.symbol, m._marker_key, m.source
having count(distinct mm._marker_key) > 1
order by m.source, numHuman desc
limit 100
