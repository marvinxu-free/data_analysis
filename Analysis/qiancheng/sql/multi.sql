select a.maxent_id, case when b.mac is not null then 1 else 0 end as mac_loan
,case when c.imei is not null then 1 else 0 end as imei_loan
,case when d.aid is not null then 1 else 0 end as aid_loan
,case when e.idfa is not null then 1 else 0 end as idfa_loan
from
(select maxent_id, mac, imei, aid, idfa from fdata.id_system_maxentid
where  day >= 20170715 and day <= 20170815 and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k') a
left join
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
left join
(select distinct a.imei from
(select imei, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and imei not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by imei) a join (
select imei from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.imei = b.imei
where a.num > 1) c on a.imei = c.imei
left join
(select distinct a.aid from
(select aid, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and aid not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by aid) a join (
select aid from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.aid = b.aid
where a.num > 1) d on a.aid = d.aid
LEFT JOIN
(select distinct a.idfa from
(select idfa, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and idfa not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by idfa) a join (
select idfa from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.idfa = b.idfa
where a.num > 1) e on a.idfa = e.idfa
