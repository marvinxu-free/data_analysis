-- 一机多贷
--imei
select a.maxent_id, case when b.imei is not null then 1 else 0 end as imei_loan from
(select maxent_id, imei from fdata.id_system_maxentid
where  day >= 20170715 and day <= 20170815 and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k') a left join
(select distinct a.imei from
(select imei, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and imei not in ('unknown',
'812345678912345',
'unknown',
'0',
'00',
'000000000000000')
group by imei) a join (
select imei from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.imei = b.imei
where a.num > 1) b on a.imei = b.imei


--mac
select a.maxent_id, case when b.mac is not null then 1 else 0 end as mac_loan from
(select maxent_id, mac from fdata.id_system_maxentid
where  day >= 20170715 and day <= 20170815 and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k') a left join
(select distinct a.mac from
(select mac, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and mac not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by mac) a join (
select mac from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.mac = b.mac
where a.num > 1) b on a.mac = b.mac

-- aid
select distinct a.aid from
(select aid, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
group by aid) a
join (select aid from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.aid = b.aid
where a.num > 1




-- idfv
select distinct a.idfv from
(select idfv, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c8a4db5fa1b93b7fb9f31b815a6cf611',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and idfv != '00000000-0000-0000-0000-000000000000'
group by idfv) a
join (select idfv from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.idfv = b.idfv
where a.num > 1



use odata;
create external table IF NOT EXISTS qiancheng_fraud
(
maxent_id string comment 'maxent_id'
)
comment 'qiancheng_fraud_data'
row format delimited
fields terminated by '\t'
location 'hdfs://hdnn/user/hive/warehouse/odata.db/qiancheng_fraud'


set hive.cli.print.header=true;
select count(*) from (
select * from odata.qiancheng_all
) a join (
select * from odata.qiancheng_fraud
) b on a.maxent_id = b.maxent_id
where a.imei_counts > 2 or a.mac_counts > 1 or a.mcid_counts > 5
or a.aid_counts > 1 or a.idfv_counts > 3 or a.idfa_counts > 3







select a.maxent_id, case when b.imei is not null then 1 else 0 end as imei_loan from

(select maxent_id, imei from fdata.id_system_maxentid
where  day >= 20170715 and day <= 20170815 and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k') a left join
(select distinct a.imei from
(select imei, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and imei not in ('unknown',
'812345678912345',
'unknown',
'0',
'00',
'000000000000000')
group by imei) a join (
select imei, mac, aid, idfa, idfv from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.imei = b.imei
where a.num > 1) b on a.imei = b.imei