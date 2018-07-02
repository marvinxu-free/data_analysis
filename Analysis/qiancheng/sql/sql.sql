SELECT count(*) FROM
(select * from odata.qiancheng_all
where imei_counts > 2
or mac_counts > 1
or mcid_counts > 5
or aid_counts > 1
or idfv_counts >3
or idfa_counts > 3) a right JOIN
(select * from odata.qiancheng_fraud) b on a.maxent_id = b.maxent_id
where a.maxent_id is NULL

SELECT COUNT(DISTINCT b.maxent_id) FROM
(select * from odata.qiancheng_all
where imei_counts > 2
or mac_counts > 1
or mcid_counts > 5
or aid_counts > 1
or idfv_counts >3
or idfa_counts > 3) a LEFT JOIN
(select * from odata.qiancheng_fraud) b
ON a.maxent_id = b.maxent_id
WHERE a.maxent_id is NOT NULL


SELECT COUNT(b.maxent_id) FROM
(SELECT DISTINCT(c.maxent_id) FROM
(select get_json_object(json, '$.maxent_id') as maxent_id
   from fr_analysis.event_maxentid_original
   where day>=20170801 and day<= 20170830 and get_json_object(json, '$.tid') = '79rctxsz5nkuc9dd9qax3ewf5hay691k' ) c) a JOIN
(SELECT maxent_id FROM odata.qiancheng_fraud_new) b
ON a.maxent_id = b.maxent_id
