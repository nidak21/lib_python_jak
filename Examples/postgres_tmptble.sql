-- Postgres SQL example to create and use a temp table in phpPgAdmin tool
create temporary table tmp_markers
as
SELECT   m.symbol, count(distinct va._annot_key) as "Num_rolled_MP"
FROM mrk_marker m inner join mrk_notes mn on  /* markers w/ marker clip */
               (m._marker_key = mn._marker_key and mn.sequencenum = 1) 
     left join voc_annot va on		/* rolled up MP annotations */
	 (m._marker_key = va._object_key and va._annottype_key = 1015)
group by m.symbol
ORDER BY "Num_rolled_MP" desc;

select count(*) from tmp_markers;

create index idx1 on tmp_markers(symbol)

select *
from tmp_markers
where symbol like 'Pax%';
