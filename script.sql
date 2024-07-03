\! echo '#### Script Version'
select 'ScriptVer:v004' as scriptversion;
\! echo '#### Cluster Report'
with sc as (select node, count(1) as slice_count from stv_slices group by 1), snodeinfo as (select node_type as type, host as node, sum(used_mb)/1024 as used_gb, sum(nominal_mb)/1024 as capacity_gb, (sum(used_mb)/sum(nominal_mb))*100 as pct_used from ( select case when capacity = 361859 or (capacity=190633 and mount ilike '/dev/nvme%') then 'dc2.large' when capacity = 381407 or capacity = 190633 then 'dc1.large' when capacity = 380319 then 'dc1.8xlarge' when capacity = 760956 then 'dc2.8xlarge' when capacity in (1906314,952455) then 'ds2.xlarge' when capacity = 945026 then 'ds2.8xlarge' when capacity = 3339176 then 'ra3.16xlarge//ra3.4xlarge' else null end as node_type, host, sum(used-tossed) as used_mb, nominal as nominal_mb, round(sum(used-tossed)/nominal*100,2) as pct_nominal_mb from ( select host, mount, used::numeric, tossed::numeric, capacity::numeric, case when capacity in (381407,190633,361859) then 160*1024 when capacity in (380319,760956) then 2.56*1024*1024 when capacity in (1906314,952455) then 2*1024*1024 when capacity = 945026 then 16*1024*1024 when capacity = 3339176 then 64*1024*1024 else null end::numeric as nominal from stv_partitions where part_begin=0 and failed=0 and capacity != 0) group by 1,2,4 ) group by 1,2) select * from snodeinfo inner join sc using (node) order by 1, 2;
\! echo '#### Schemas Report'
SELECT TRIM(db_name) AS db_name ,TRIM(schema_name) AS schema_name ,TO_CHAR(SUM(rows_total),'999,999,999,999,999') rows_total ,TO_CHAR(SUM(COALESCE(size_in_gb,0)),'999,999,999,999,999') size_in_gb FROM (SELECT id table_id ,datname db_name ,nspname schema_name ,relname table_name ,SUM(rows) rows_total ,SUM(sorted_rows) rows_sorted FROM stv_tbl_perm JOIN pg_class ON pg_class.oid = stv_tbl_perm.id JOIN pg_namespace ON pg_namespace.oid = relnamespace JOIN pg_database ON pg_database.oid = stv_tbl_perm.db_id WHERE name NOT LIKE 'pg_%' AND name NOT LIKE 'stl_%' AND name NOT LIKE 'stp_%' AND name NOT LIKE 'padb_%' AND nspname <> 'pg_catalog' GROUP BY id, datname, nspname, relname ORDER BY id, datname, nspname, relname) tbl_det LEFT JOIN (SELECT tbl table_id ,ROUND(CONVERT(REAL,COUNT(*))/1024,2) size_in_gb FROM stv_blocklist bloc GROUP BY tbl) tbl_size ON tbl_size.table_id = tbl_det.table_id GROUP BY 1,2 ORDER BY size_in_gb DESC;
\! echo '#### Large Tables Report'
SELECT database, schema, "table", encoded, diststyle, sortkey1, max_varchar, sortkey1_enc, sortkey_num, TO_CHAR(size,'999,999,999,999,999') size, pct_used, empty, unsorted, stats_off, TO_CHAR(tbl_rows,'999,999,999,999,999') tbl_rows, skew_sortkey1, skew_rows, TO_CHAR(estimated_visible_rows,'999,999,999,999') estimated_visible_rows, risk_event, vacuum_sort_benefit FROM svv_table_info ORDER BY size desc LIMIT 25;
\! echo '#### Skewed Tables Report'
SELECT database, schema, "table", encoded, diststyle, sortkey1, max_varchar, sortkey1_enc, sortkey_num, TO_CHAR(size,'999,999,999,999,999') size, pct_used, empty, unsorted, stats_off, TO_CHAR(tbl_rows,'999,999,999,999,999') tbl_rows, skew_sortkey1, skew_rows FROM svv_table_info WHERE skew_rows > 1.5 ORDER BY size desc LIMIT 25;
\! echo '#### Candidates for DISTSTYLE ALL Report'
SELECT database, schema, "table", encoded, diststyle, sortkey1, max_varchar, sortkey1_enc, sortkey_num, TO_CHAR(size,'999,999,999,999,999') size, pct_used, empty, unsorted, stats_off, TO_CHAR(tbl_rows,'999,999,999,999,999') tbl_rows, skew_sortkey1, skew_rows, (tbl_rows :: NUMERIC / size :: NUMERIC) rows_per_mb FROM  svv_table_info WHERE (tbl_rows :: NUMERIC / size :: NUMERIC) < 100 AND tbl_rows < 1000000 ORDER BY /*(tbl_rows::NUMERIC/size::NUMERIC)*/ size desc  LIMIT 25 ;
\! echo '#### Alerts Report'
WITH stl_scan_filt as (SELECT * FROM stl_scan WHERE perm_table_name NOT ilike '%Internal Worktable' AND perm_table_name NOT ilike '%S3 Partition Scan custom_catchall_upgrade' AND perm_table_name NOT ilike '%S3 Subquery custom_catchall_upgrade') SELECT trim(s.perm_table_name) AS TABLE ,(sum(abs(datediff(seconds, coalesce(b.starttime, d.starttime, s.starttime), CASE WHEN coalesce(b.endtime, d.endtime, s.endtime) > coalesce(b.starttime, d.starttime, s.starttime) THEN coalesce(b.endtime, d.endtime, s.endtime) ELSE coalesce(b.starttime, d.starttime, s.starttime) END))) / 60)::NUMERIC(24, 0) AS minutes ,sum(coalesce(b.rows, d.rows, s.rows)) AS rows ,trim(split_part(l.event, ':', 1)) AS event ,substring(trim(l.solution), 1, 60) AS solution ,max(l.query) AS sample_query ,count(DISTINCT l.query) FROM stl_alert_event_log AS l LEFT JOIN stl_scan_filt AS s ON s.query = l.query AND s.slice = l.slice AND s.segment = l.segment LEFT JOIN stl_dist AS d ON d.query = l.query AND d.slice = l.slice AND d.segment = l.segment LEFT JOIN stl_bcast AS b ON b.query = l.query AND b.slice = l.slice AND b.segment = l.segment WHERE l.userid > 1 AND l.event_time >= dateadd(day, - 7, CURRENT_DATE) GROUP BY 1 ,4 ,5 ORDER BY 2 DESC ,6 DESC LIMIT 25;
\! echo '#### Unused Tables Report'
SELECT database ,schema ,"table" ,size ,sortkey1 ,NVL(s.num_qs, 0) num_queries FROM svv_table_info t LEFT JOIN (SELECT tbl ,perm_table_name ,COUNT(DISTINCT query) num_qs FROM stl_scan s WHERE s.userid > 1 AND s.perm_table_name NOT IN ('Internal Worktable','S3') GROUP BY 1,2 ) s ON s.tbl = t.table_id WHERE NVL(s.num_qs, 0) = 0 ORDER BY size DESC LIMIT 25;

\! echo '#### Top Disk Spilling Queries'
select userid, query, starttime,endtime, xid, aborted, ROUND(query_temp_blocks_to_disk::decimal/1024::decimal,3) spilled_gb, substring(querytxt,1,150) from stl_query join (select query, query_temp_blocks_to_disk from svl_query_metrics_summary where query_temp_blocks_to_disk > 1024 order by 2 desc nulls last limit 50) a USING (query) WHERE userid>1 order by 7 desc limit 25;

\! echo '#### Query Disk Full Errors'
select TIMESTAMP'epoch'+(((a.currenttime/1000000)+946684800)*INTERVAL'1 SECOND')as event_time,node_num,query_id,temp_blocks,starttime,endtime,datediff(s,starttime,endtime)as duration_s,aborted,substring(b.querytxt,1,75)as querytext from stl_disk_full_diag a left join stl_query b on query_id=query order by starttime limit 25;
\! echo '#### Top 50 Queries Report'
SELECT 
  TRIM("database") AS DB, 
  COUNT(query) AS n_qry, 
  MAX(SUBSTRING(qrytext, 1, 120)) AS qrytext, 
  MAX(SUBSTRING(solution, 1, 25)) as solution, 
  MIN(exec_secconds) AS min_exec_seconds, 
  MAX(exec_secconds) AS max_exec_seconds, 
  AVG(exec_secconds) AS avg_exec_seconds, 
  SUM(exec_secconds) AS total_exec_seconds, 
  SUM(queue_secconds) AS total_queue_seconds, 
  MAX(query) AS max_query_id, 
  MAX(starttime)::date AS last_run, 
  aborted, 
  MAX(mylabel) qry_label, 
  TRIM(DECODE(event & 1, 1, 'Sortkey ', '') || DECODE(event & 2, 2, 'Deletes ', '') || DECODE(event & 4, 4, 'NL ', '') || DECODE(event & 8, 8, 'Dist ', '') || DECODE(event & 16, 16, 'Broadcast ', '') || DECODE(event & 32, 32, 'Stats ', '')) AS Alert 
FROM (
    SELECT 
        stl_query.userid,
        label,
        stl_query.query,
        TRIM(DATABASE) AS DATABASE,
        NVL(qrytext_cur.text, TRIM(querytxt)) AS qrytext,
        solution,
        MD5(NVL(qrytext_cur.text, TRIM(querytxt))) AS qry_md5,
        starttime,
        endtime,
        DATEDIFF(seconds, starttime, endtime)::NUMERIC(12,2) AS run_seconds,
        ROUND(total_queue_time / 1000000.0, 2) AS queue_secconds,
        ROUND(total_exec_time / 1000000.0, 2) AS exec_secconds,
        aborted,
        event,
        stl_query.label AS mylabel
    FROM stl_query JOIN stl_wlm_query ON stl_query.query = stl_wlm_query.query
    LEFT OUTER JOIN (
        SELECT 
            query,
            solution,
            SUM(DECODE(TRIM(SPLIT_PART(event, ':', 1) ), 'Very selective query filter', 1, 'Scanned a large number of deleted rows' , 2, 'Nested Loop Join in the query plan' , 4, 'Distributed a large number of rows across the network', 8, 'Broadcasted a large number of rows across the network', 16, 'Missing query planner statistics', 32, 0)) AS event
        FROM stl_alert_event_log WHERE event_time >= DATEADD(day, -7, CURRENT_DATE) GROUP BY query, solution
    ) AS alrt ON alrt.query = stl_query.query
    LEFT OUTER JOIN (
        SELECT
            ut.xid, 
            TRIM(SUBSTRING (text FROM STRPOS(UPPER( text), 'SELECT')) ) AS TEXT
        FROM stl_utilitytext ut WHERE sequence = 0 AND UPPER(text) LIKE 'DECLARE%' GROUP BY text, ut.xid
    ) qrytext_cur ON ( stl_query.xid = qrytext_cur.xid )
WHERE stl_query.userid <> 1 AND starttime >= DATEADD(day, -2, CURRENT_DATE))
GROUP BY DATABASE, userid, label, qry_md5, aborted, event ORDER BY total_exec_seconds DESC LIMIT 50;

\! echo '#### WLM Queue Configuration Report'
SELECT wlm.service_class AS queue,
       CASE
         WHEN LEN (TRIM(wlm.name)) > 20 THEN '?' ||RIGHT (TRIM(wlm.name),19)
         ELSE TRIM(wlm.name)
       END AS queue_name,
       LISTAGG(TRIM(cnd.condition),', ') AS condition,
       wlm.num_query_tasks AS query_concurrency,
       wlm.query_working_mem AS per_query_memory_mb,
       ROUND(((wlm.num_query_tasks*wlm.query_working_mem)::NUMERIC/ mem.total_mem::NUMERIC)*100,0)::INT AS cluster_memory_pct,
       wlm.max_execution_time,
       wlm.user_group_wild_card,
       wlm.query_group_wild_card,
       TRIM(wlm.concurrency_scaling) AS cs_enabled,
       TRIM(wlm.query_priority) AS priority
FROM stv_wlm_service_class_config wlm
  INNER JOIN stv_wlm_classification_config cnd ON wlm.service_class = cnd.action_service_class
  CROSS JOIN (SELECT SUM(num_query_tasks*query_working_mem) AS total_mem
              FROM pg_catalog.stv_wlm_service_class_config
              WHERE service_class BETWEEN 6 AND 13) mem
WHERE wlm.service_class >= 6
GROUP BY 1,2,4,5,6,7,8,9,10,11
ORDER BY 1;

\! echo '#### WLM Query Management Rules (QMR) Report'
SELECT qmr.service_class queue ,TRIM(wlm.name) queue_name ,TRIM(rule_name) rule_name ,TRIM(action) AS action ,TRIM(metric_name)||' '||TRIM(metric_operator)||' '||metric_value AS rule FROM stv_wlm_qmr_config qmr JOIN stv_wlm_service_class_config wlm USING (service_class) WHERE qmr.service_class > 5 ORDER BY qmr.service_class,TRIM(rule_name);
\! echo '#### WLM Hourly Peak Concurrent Queries Report'
WITH generate_dt_series AS ( SELECT SYSDATE-(n*INTERVAL '5 second') AS dt FROM (SELECT ROW_NUMBER() OVER () AS n FROM stl_scan LIMIT 120960) ), apex AS ( SELECT iq.dt, iq.service_class, iq.num_query_tasks, COUNT(iq.slot_count) AS service_class_queries, SUM(iq.slot_count) AS service_class_slots FROM (SELECT gds.dt, wq.service_class, wscc.num_query_tasks, wq.slot_count FROM stl_wlm_query wq JOIN stv_wlm_service_class_config wscc ON (wscc.service_class = wq.service_class AND wscc.service_class > 4) JOIN generate_dt_series gds ON (wq.service_class_start_time <= gds.dt AND wq.service_class_end_time > gds.dt) WHERE wq.userid > 1 AND wq.service_class > 4) iq GROUP BY iq.dt, iq.service_class, iq.num_query_tasks ), maxes AS ( SELECT apex.service_class, trunc(apex.dt) AS d, to_char(apex.dt,'HH24') AS dt_h, MAX(service_class_slots) max_service_class_slots FROM apex GROUP BY apex.service_class, apex.dt, to_char(apex.dt,'HH24') ), apexes AS ( SELECT apex.service_class, apex.num_query_tasks AS max_wlm_concurrency, maxes.d AS day, maxes.dt_h || ':00 - ' || maxes.dt_h || ':59' AS hour, MAX(apex.service_class_slots) AS max_service_class_slots FROM apex JOIN maxes ON (apex.service_class = maxes.service_class AND apex.service_class_slots = maxes.max_service_class_slots) GROUP BY apex.service_class, apex.num_query_tasks, maxes.d, maxes.dt_h ORDER BY apex.service_class, maxes.d, maxes.dt_h ) SELECT service_class ,"hour" ,MAX(CASE WHEN "day" = DATE_TRUNC('day',GETDATE()) THEN max_service_class_slots ELSE NULL END) today ,MAX(CASE WHEN "day" = DATE_TRUNC('day',GETDATE())-1 THEN max_service_class_slots ELSE NULL END) yesterday ,MAX(CASE WHEN "day" = DATE_TRUNC('day',GETDATE())-2 THEN max_service_class_slots ELSE NULL END) two_days_ago ,MAX(CASE WHEN "day" = DATE_TRUNC('day',GETDATE())-3 THEN max_service_class_slots ELSE NULL END) three_days_ago ,MAX(CASE WHEN "day" = DATE_TRUNC('day',GETDATE())-4 THEN max_service_class_slots ELSE NULL END) four_days_ago ,MAX(CASE WHEN "day" = DATE_TRUNC('day',GETDATE())-5 THEN max_service_class_slots ELSE NULL END) five_days_ago ,MAX(CASE WHEN "day" = DATE_TRUNC('day',GETDATE())-6 THEN max_service_class_slots ELSE NULL END) six_days_ago FROM apexes GROUP BY service_class ,"hour" ORDER BY service_class ,"hour" ;
\! echo '#### QMR Rule Candidates - By Service Class Report'
WITH qmr AS (
              SELECT service_class, 'query_cpu_time'            ::VARCHAR(30) qmr_metric, MEDIAN(query_cpu_time            ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_cpu_time            ) p99, MAX(query_cpu_time            ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_blocks_read'         ::VARCHAR(30) qmr_metric, MEDIAN(query_blocks_read         ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_blocks_read         ) p99, MAX(query_blocks_read         ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_execution_time'      ::VARCHAR(30) qmr_metric, MEDIAN(query_execution_time      ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_execution_time      ) p99, MAX(query_execution_time      ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_queue_time'          ::VARCHAR(30) qmr_metric, MEDIAN(query_queue_time          ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_queue_time          ) p99, MAX(query_execution_time      ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_cpu_usage_percent'   ::VARCHAR(30) qmr_metric, MEDIAN(query_cpu_usage_percent   ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_cpu_usage_percent   ) p99, MAX(query_cpu_usage_percent   ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_temp_blocks_to_disk' ::VARCHAR(30) qmr_metric, MEDIAN(query_temp_blocks_to_disk ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_temp_blocks_to_disk ) p99, MAX(query_temp_blocks_to_disk ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'segment_execution_time'    ::VARCHAR(30) qmr_metric, MEDIAN(segment_execution_time    ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY segment_execution_time    ) p99, MAX(segment_execution_time    ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'cpu_skew'                  ::VARCHAR(30) qmr_metric, MEDIAN(cpu_skew                  ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY cpu_skew                  ) p99, MAX(cpu_skew                  ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'io_skew'                   ::VARCHAR(30) qmr_metric, MEDIAN(io_skew                   ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY io_skew                   ) p99, MAX(io_skew                   ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'scan_row_count'            ::VARCHAR(30) qmr_metric, MEDIAN(scan_row_count            ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY scan_row_count            ) p99, MAX(scan_row_count            ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'join_row_count'            ::VARCHAR(30) qmr_metric, MEDIAN(join_row_count            ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY join_row_count            ) p99, MAX(join_row_count            ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'nested_loop_join_row_count'::VARCHAR(30) qmr_metric, MEDIAN(nested_loop_join_row_count) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY nested_loop_join_row_count) p99, MAX(nested_loop_join_row_count) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'return_row_count'          ::VARCHAR(30) qmr_metric, MEDIAN(return_row_count          ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY return_row_count          ) p99, MAX(return_row_count          ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'spectrum_scan_row_count'   ::VARCHAR(30) qmr_metric, MEDIAN(spectrum_scan_row_count   ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY spectrum_scan_row_count   ) p99, MAX(spectrum_scan_row_count   ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'spectrum_scan_size_mb'     ::VARCHAR(30) qmr_metric, MEDIAN(spectrum_scan_size_mb     ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY spectrum_scan_size_mb     ) p99, MAX(spectrum_scan_size_mb     ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1    
)
SELECT service_class
      ,qmr_metric,p50,p99,pmax
      ,(LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT)) candidate_rule
      ,ROUND(pmax/((LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT))),2) pmax_magnitude
      ,ROW_NUMBER() OVER (PARTITION BY service_class ORDER BY (NVL(pmax,1)/((LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT)))) DESC) rule_order
FROM   qmr
WHERE NVL(p99,0) >= 10 
AND (NVL(p50,0) + NVL(p99,0)) < NVL(pmax,0) 
AND ((LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT))) < NVL(pmax,0) 
ORDER BY service_class
        ,rule_order;
\! echo '#### Copy Performance Per Table Report'
SELECT a.endtime::date,a.tbl,trim(c.nspname) as "schema", trim(b.relname) as "tablename", sum(a.rows_inserted) as "rows_inserted", sum(d.distinct_files) as files_scanned, sum(d.MB_scanned) as MB_scanned, (sum(d.distinct_files)::numeric(19,3)/count(distinct a.query)::numeric(19,3))::numeric(19,3) as avg_files_per_copy, (sum(d.MB_scanned)/sum(d.distinct_files)::numeric(19,3))::numeric(19,3) as avg_file_size_mb, count(distinct a.query) no_of_copy, max(a.query) as sample_query , (sum(d.MB_scanned)*1024*1000000/SUM(d.load_micro)) as scan_rate_kbps, (sum(a.rows_inserted)*1000000/SUM(a.insert_micro)) as insert_rate_rows_ps from ( select query, tbl, sum(rows) as rows_inserted, max(endtime) as endtime, datediff('microsecond',min(starttime),max(endtime)) as insert_micro from stl_insert group by query, tbl) a, pg_class b, pg_namespace c , (select b.query, count(distinct b.bucket||b.key) as distinct_files, sum(b.transfer_size)/1024/1024 as MB_scanned, sum(b.transfer_time) as load_micro from stl_s3client b where b.http_method = 'GET' group by b.query) d where a.tbl = b.oid and b.relnamespace = c.oid and d.query = a.query group by 1,2,3,4 order by 1 desc, 5 desc, 3,4 LIMIT 25;
\! echo '#### Copy Time Spent on Compression and Statistics Report'
SELECT MAX(a.query) last_query ,MAX(a.xid) last_xid ,COUNT(*) load_count ,ROUND(SUM(COALESCE(b.comp_time, 0)) / 1000.00, 0) compression_secs ,ROUND(SUM(COALESCE(a.copy_time, 0)) / 1000.00, 0) copy_load_secs ,ROUND(SUM(COALESCE(c.analyze_time, 0)) / 1000.00, 0) analyse_secs ,SUBSTRING(q.querytxt, 1, 150) FROM ( SELECT query ,xid ,datediff(ms, starttime, endtime) copy_time FROM stl_query q WHERE (querytxt ilike 'copy %from%') AND EXISTS ( SELECT 1 FROM stl_commit_stats cs WHERE cs.xid = q.xid ) AND EXISTS ( SELECT xid FROM stl_query WHERE query IN ( SELECT DISTINCT query FROM stl_load_commits ) ) ) a LEFT JOIN ( SELECT xid ,SUM(datediff(ms, starttime, endtime)) comp_time FROM stl_query q WHERE ( querytxt LIKE 'COPY ANALYZE %' OR querytxt LIKE 'analyze compression phase %' ) AND EXISTS ( SELECT 1 FROM stl_commit_stats cs WHERE cs.xid = q.xid ) AND EXISTS ( SELECT xid FROM stl_query WHERE query IN ( SELECT DISTINCT query FROM stl_load_commits ) ) GROUP BY 1 ) b ON b.xid = a.xid LEFT JOIN ( SELECT xid ,SUM(datediff(ms, starttime, endtime)) analyze_time FROM stl_query q WHERE (querytxt LIKE 'padb_fetch_sample%') AND EXISTS ( SELECT 1 FROM stl_commit_stats cs WHERE cs.xid = q.xid ) AND EXISTS ( SELECT xid FROM stl_query WHERE query IN ( SELECT DISTINCT query FROM stl_load_commits ) ) GROUP BY 1 ) c ON c.xid = a.xid INNER JOIN stl_query q ON q.query = a.query WHERE (b.comp_time IS NOT NULL) OR (c.analyze_time > a.copy_time) GROUP BY SUBSTRING(q.querytxt, 1, 150) ORDER BY (ROUND(SUM(COALESCE(b.comp_time, 0)) / 1000.00, 0) + ROUND(SUM(COALESCE(a.copy_time, 0)) / 1000.00, 0) + ROUND(SUM(COALESCE(c.analyze_time, 0)) / 1000.00, 0)) DESC LIMIT 25;

\! echo '#### WLM Concurrency'
WITH generate_dt_series AS(SELECT sysdate-(n*interval'1 second')AS dt FROM(SELECT row_number()OVER()AS n FROM stl_scan limit 604800)),apex AS(SELECT iq.dt,iq.service_class,iq.num_query_tasks,count(iq.slot_count)AS service_class_queries,sum(iq.slot_count)AS service_class_slots FROM(SELECT gds.dt,wq.service_class,wscc.num_query_tasks,wq.slot_count FROM stl_wlm_query wq JOIN stv_wlm_service_class_config wscc ON(wscc.service_class=wq.service_class AND wscc.service_class>4)JOIN generate_dt_series gds ON(wq.service_class_start_time<=gds.dt AND wq.service_class_end_time>gds.dt)WHERE wq.userid>1 AND wq.service_class>4)iq GROUP BY iq.dt,iq.service_class,iq.num_query_tasks),maxes AS(SELECT apex.service_class,trunc(apex.dt)AS d,date_part(h,apex.dt)AS dt_h,max(service_class_slots)max_service_class_slots FROM apex GROUP BY apex.service_class,apex.dt,date_part(h,apex.dt))SELECT apex.service_class,apex.num_query_tasks AS max_wlm_concurrency,maxes.d AS day,maxes.dt_h||':00 - '||maxes.dt_h||':59' AS hour,max(apex.service_class_slots)AS max_service_class_slots_requested FROM apex JOIN maxes ON(apex.service_class=maxes.service_class AND apex.service_class_slots=maxes.max_service_class_slots)GROUP BY apex.service_class,apex.num_query_tasks,maxes.d,maxes.dt_h ORDER BY apex.service_class,maxes.d,maxes.dt_h;

\! echo '#### Node Check'
with query_report_data as(select qr.start_time::date as day_d,sl.node,qr.slice,qr.query,qr.segment,datediff('ms',min(qr.start_time),max(qr.end_time))as elapsed_ms,sum(qr.bytes)as bytes from svl_query_report qr join stv_slices as sl on(sl.slice=qr.slice)where qr.end_time>qr.start_time group by day_d,sl.node,qr.slice,qr.query,qr.segment)select day_d,node,sum(elapsed_ms)as elapsed_ms,sum(bytes)as bytes,round(ratio_to_report(sum(elapsed_ms))over(partition by day_d),2)*100 as pct_elapsed_day,round(ratio_to_report(sum(bytes))over(partition by day_d),2)*100 as pct_bytes_day from query_report_data group by day_d,node order by day_d limit 50;

\! echo '#### Redshift Version'
select version() as rs_version, current_database() as database_name;

\! echo '#### Concurrency Scaling Usage'
select * FROM SVCS_CONCURRENCY_SCALING_USAGE order by start_time desc limit 25;

\! echo '#### Concurrency Scaling Queries'
SELECT w.service_class AS queue, q.concurrency_scaling_status, COUNT( * ) AS queries, SUM( q.aborted )  AS aborted, SUM( ROUND(total_queue_time::NUMERIC / 1000000,2 ) ) AS queue_secs, SUM( ROUND( total_exec_time::NUMERIC / 1000000,2 ) )  AS exec_secs FROM stl_query q JOIN stl_wlm_query w USING (userid,query) WHERE q.userid > 1 AND q.starttime > date_trunc('d', sysdate-7) GROUP BY 1,2 ORDER BY 1,2 limit 25;

\! echo '#### Cluster Base Data'
select * FROM stv_partitions WHERE OWNER = host ORDER BY OWNER ,host ,diskno;

\! echo '#### Most uncompressed data tables'
SELECT ti.schema||'.'||ti."table" tablename, raw_size.size uncompressed_mb, ti.size total_mb FROM svv_table_info ti LEFT JOIN ( SELECT tbl table_id, COUNT(*) size FROM stv_blocklist WHERE (tbl,col) IN ( SELECT attrelid, attnum-1 FROM pg_attribute WHERE attencodingtype IN (0,128) AND attnum>0 AND attsortkeyord != 1) GROUP BY tbl) raw_size USING (table_id) WHERE raw_size.size IS NOT NULL ORDER BY raw_size.size DESC LIMIT 25;

\! echo '#### Copy loading large uncompressed data'
SELECT wq.userid, query, exec_start_time AS starttime, COUNT(*) num_files, ROUND(MAX(wq.total_exec_time/1000000.0),2) execution_secs, ROUND(SUM(transfer_size)/(1024.0*1024.0),2) total_mb, SUBSTRING(querytxt,1,60) copy_sql FROM stl_s3client s JOIN stl_query q USING (query) JOIN stl_wlm_query wq USING (query) WHERE s.userid>1 AND http_method = 'GET' AND POSITION('COPY ANALYZE' IN querytxt) = 0 AND aborted = 0 AND final_state='Completed' GROUP BY 1, 2, 3, 7 HAVING SUM(transfer_size) = SUM(data_size)  AND SUM(transfer_size)/(1024*1024) >= 5 ORDER BY 6 DESC, 5 DESC LIMIT 25;

\! echo '#### Predicate Columns For Analyze'
WITH predicate_column_info as (SELECT ns.nspname AS schema_name, c.relname AS table_name, a.attnum as col_num,  a.attname as col_name, CASE WHEN 10002 = s.stakind1 THEN array_to_string(stavalues1, '||') WHEN 10002 = s.stakind2 THEN array_to_string(stavalues2, '||') WHEN 10002 = s.stakind3 THEN array_to_string(stavalues3, '||') WHEN 10002 = s.stakind4 THEN array_to_string(stavalues4, '||') ELSE NULL::varchar END AS pred_ts FROM pg_statistic s JOIN pg_class c ON c.oid = s.starelid JOIN pg_namespace ns ON c.relnamespace = ns.oid JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = s.staattnum) SELECT schema_name, table_name, col_num, col_name, pred_ts NOT LIKE '2000-01-01%' AS is_predicate, CASE WHEN pred_ts NOT LIKE '2000-01-01%' THEN (split_part(pred_ts, '||',1))::timestamp ELSE NULL::timestamp END as first_predicate_use, CASE WHEN pred_ts NOT LIKE '%||2000-01-01%' THEN (split_part(pred_ts, '||',2))::timestamp ELSE NULL::timestamp END as last_analyze FROM predicate_column_info where is_predicate='true' LIMIT 25;

\! echo '#### Advisor Alter Table Recommendations'
with tab_dtls as (select table_id, "table" table_name from svv_table_info) select type, database, table_id, table_name, group_id, ddl, auto_eligible from svv_alter_table_recommendations r inner join tab_dtls d using(table_id);

\! echo '#### Automatic Actions'
with tab_dtls as (select table_id, "table" table_name from svv_table_info) select table_id, table_name, type, status, eventtime, sequence, previous_state from SVL_AUTO_WORKER_ACTION a inner join tab_dtls d using(table_id) order by eventtime;

\! echo '#### Spectrum Queries Summary'
select query, min(starttime) starttime, max(endtime) endtime, datediff(seconds, min(starttime), max(endtime)) exec_time_secs, sum(s3_scanned_rows) s3_scanned_rows, sum(s3_scanned_bytes) s3_scanned_bytes, sum(s3query_returned_rows) s3query_returned_rows, sum(s3query_returned_bytes) s3query_returned_bytes, sum(files) num_files, sum(total_partitions) total_partitions, sum(qualified_partitions) qualified_partitions from svcs_s3query_summary left join svcs_s3partition_summary using (query) group by query order by starttime desc limit 25;

\! echo '#### MV Summary'
with mv_info as ( select (db_name || "schema" || name) as mvw_name, is_stale, state, case state when 0 then 'The MV is fully recomputed when refreshed' when 1 then 'The MV is incremental' when 101 then 'The MV cant be refreshed due to a dropped column. This constraint applies even if the column isnt used in the MV' when 102 then 'The MV cant be refreshed due to a changed column type. This constraint applies even if the column isnt used in the MV' when 103 then 'The MV cant be refreshed due to a renamed table' when 104 then 'The MV cant be refreshed due to a renamed column. This constraint applies even if the column isnt used in the MV' when 105 then 'The MV cant be refreshed due to a renamed schema' else null end as state_desc, autorewrite, autorefresh from stv_mv_info), mv_state as ( select mvw_name, state as mv_state, event_desc,starttime as evt_starttime from ( select (db_name || mv_schema || mv_name) as mvw_name, row_number() over(partition by mvw_name order by starttime desc) as rnum, state, event_desc, starttime from stl_mv_state) where rnum = 1 ), mv_ref_status as ( select mvw_name, status, refresh_type, starttime as ref_starttime, endtime as ref_endtime from ( select (db_name || schema_name || mv_name) as mvw_name, row_number() over(partition by mvw_name order by starttime desc) as rnum, status, refresh_type, starttime, endtime from svl_mv_refresh_status) where rnum = 1 ) select * from mv_info left join mv_state using (mvw_name) left join mv_ref_status using (mvw_name) limit 25;

\! echo '#### Skewed Query Summary'
with scan as (
    select userid, query, slice, starttime, endtime, datediff(sec,starttime,endtime) elapsed_time, rows, bytes,rows_pre_filter
    from stl_scan
    where segment = 0
      and step = 0
      and slice in (0,1)
    and userid>1
)select a.userid,a.query,a.slice,a.starttime,a.endtime,a.elapsed_time elapsed_slice_0, b.elapsed_time elapsed_slice_1, (case when b.elapsed_time > 0 then a.elapsed_time / b.elapsed_time else 0 end) duration_diff, a.rows rows_slice_0,b.rows rows_slice_1, a.rows_pre_filter rows_pre_filter_slice_0,b.rows_pre_filter rows_pre_filter_slice_1,substring(querytxt,1,50)
from scan a inner join scan b using (query)
inner join stl_query q using(query)
where a.slice = 0 and b.slice = 1
order by duration_diff desc LIMIT 20;

\! echo '#### Query Execution Time Buckets for the last 7 days'
WITH vw_query_dtls AS
(
SELECT
    aborted, q.userid,querytxt,q.query,
    date_trunc('day', q.starttime) AS "day",
    nvl(w.service_class,0) AS service_class,
    datediff(MICROSECOND, q.starttime, q.endtime)/(1000*1000) total_secs,
    nvl(w.total_queue_time,0) / (1000*1000) AS queue_secs,
  CASE 
  WHEN q.userid = 1 THEN 'SYSTEM' WHEN REGEXP_INSTR("querytxt",'(padb_|pg_internal)' ) THEN 'SYSTEM' 
  WHEN REGEXP_INSTR("querytxt",'[uU][nN][dD][oO][iI][nN][gG] ') THEN 'ROLLBACK' 
  WHEN REGEXP_INSTR("querytxt",'[cC][uU][rR][sS][oO][rR] ' ) THEN 'CURSOR' WHEN REGEXP_INSTR("querytxt",'[fF][eE][tT][cC][hH] ' ) THEN 'CURSOR' 
  WHEN REGEXP_INSTR("querytxt",'[dD][eE][lL][eE][tT][eE] ' ) THEN 'DELETE' WHEN REGEXP_INSTR("querytxt",'[cC][oO][pP][yY] ' ) THEN 'COPY' WHEN 
  REGEXP_INSTR("querytxt",'[uU][pP][dD][aA][tT][eE] ' ) THEN 'UPDATE' WHEN REGEXP_INSTR("querytxt",'[iI][nN][sS][eE][rR][tT] ' ) THEN 'INSERT' WHEN 
  REGEXP_INSTR("querytxt",'[vV][aA][cC][uU][uU][mM][ :]' ) THEN 'VACUUM' WHEN REGEXP_INSTR("querytxt",'[sS][eE][lL][eE][cC][tT] ' ) THEN 'SELECT' ELSE 'OTHER' END 
  as query_type
FROM stl_query q  
    LEFT JOIN stl_wlm_query w
    ON q.query = w.query
    AND q.userid = w.userid
    LEFT JOIN (SELECT query, sum(datediff (us,starttime,endtime)) AS compile_time
                    FROM svl_compile where compile=1
                    group by query) as c
    ON c.query = q.query
WHERE  
    q.starttime >= DATEADD (day,-7,CURRENT_DATE) AND q.starttime < CURRENT_DATE
  AND query_type <> 'SYSTEM'
  )
select 
    service_class, aborted, day,query_type,
  exec_bucket, count(1) as total_count, current_date
from (
select aborted, day,query_type, service_class,
    case when total_secs <= 30 then 'less_than_30_secs'
        when total_secs <= 60 then 'less_than_1_min'
        when total_secs <= 120 then 'less_than_2_mins'
        when total_secs <= 300 then 'less_than_5_mins'
        when total_secs <= 600 then 'less_than_10_mins'
        when total_secs < 1800 then 'less_than_30_mins'
        else 'more_than_30_mins' end as exec_bucket
from vw_query_dtls)
group by 1, 2, 3, 4, 5, 7
order by 2, 4, 1, 3, 5
LIMIT 20;


\! echo '#### Query Total Runtime Metrics'
WITH vw_query_dtls AS
(
SELECT
    aborted, q.userid,querytxt,q.query,
    date_trunc('day', q.starttime) AS "day",
    nvl(w.service_class,0) AS service_class,
    datediff(MICROSECOND, q.starttime, q.endtime)/(1000*1000) total_secs,
    nvl(w.total_queue_time,0) / (1000*1000) AS queue_secs,
  CASE 
  WHEN q.userid = 1 THEN 'SYSTEM' WHEN REGEXP_INSTR("querytxt",'(padb_|pg_internal)' ) THEN 'SYSTEM' 
  WHEN REGEXP_INSTR("querytxt",'[uU][nN][dD][oO][iI][nN][gG] ') THEN 'ROLLBACK' 
  WHEN REGEXP_INSTR("querytxt",'[cC][uU][rR][sS][oO][rR] ' ) THEN 'CURSOR' WHEN REGEXP_INSTR("querytxt",'[fF][eE][tT][cC][hH] ' ) THEN 'CURSOR' 
  WHEN REGEXP_INSTR("querytxt",'[dD][eE][lL][eE][tT][eE] ' ) THEN 'DELETE' WHEN REGEXP_INSTR("querytxt",'[cC][oO][pP][yY] ' ) THEN 'COPY' WHEN 
  REGEXP_INSTR("querytxt",'[uU][pP][dD][aA][tT][eE] ' ) THEN 'UPDATE' WHEN REGEXP_INSTR("querytxt",'[iI][nN][sS][eE][rR][tT] ' ) THEN 'INSERT' WHEN 
  REGEXP_INSTR("querytxt",'[vV][aA][cC][uU][uU][mM][ :]' ) THEN 'VACUUM' WHEN REGEXP_INSTR("querytxt",'[sS][eE][lL][eE][cC][tT] ' ) THEN 'SELECT' ELSE 'OTHER' END 
  as query_type
FROM stl_query q  
    LEFT JOIN stl_wlm_query w
    ON q.query = w.query
    AND q.userid = w.userid
    LEFT JOIN (SELECT query, sum(datediff (us,starttime,endtime)) AS compile_time
                    FROM svl_compile where compile=1
                    group by query) as c
    ON c.query = q.query
WHERE  
    q.starttime >= DATEADD (day,-7,CURRENT_DATE) AND q.starttime < CURRENT_DATE
  AND query_type <> 'SYSTEM'
  ),
query_metrics AS
(
    SELECT day,
        aborted,
        query_type,
        'queue_secs'::VARCHAR(30) query_metric,
        count(query) as query_count,
        percentile_cont(0.50) WITHIN GROUP(ORDER BY queue_secs) p50,
        percentile_cont(0.90) WITHIN GROUP(ORDER BY queue_secs) p90,
        percentile_cont(0.95) WITHIN GROUP(ORDER BY queue_secs) p95,
        percentile_cont(0.99) WITHIN GROUP(ORDER BY queue_secs) p99,
        max(queue_secs) pmax
    FROM vw_query_dtls
    GROUP BY 1,2,3
    UNION ALL
    SELECT  day,
        aborted,
        query_type,
        'total_secs'::VARCHAR(30) query_metric,
        count(query) as query_count,
        percentile_cont(0.50) WITHIN GROUP(ORDER BY total_secs) p50,
        percentile_cont(0.90) WITHIN GROUP(ORDER BY total_secs) p90,
        percentile_cont(0.95) WITHIN GROUP(ORDER BY total_secs) p95,
        percentile_cont(0.99) WITHIN GROUP(ORDER BY total_secs) p99,
        max(total_secs) pmax
    FROM vw_query_dtls
    GROUP BY 1,2,3
)
SELECT 
    query_metric, 
    "day",
    aborted,
    query_type,
    query_count,
    p50,
    p90,
    p95,
    p99,
    pmax,
    current_date
FROM 
    query_metrics 
ORDER BY
    query_metric,day LIMIT 20
;

