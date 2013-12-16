
60% [593 784] fixed:                  select * from mpod_audit_log where p = 20131101 and activity_type='login' and activity_data NOT LIKE '%fixed:%'
---40% more difficult:                              [402 471] select count(*) from mpod_audit_log where p = 20131101 and activity_type='login' and activity_data NOT LIKE '%fixed:%'
16% [164 454] NOWTV                   select count(*) from mpod_audit_log where p = 20131101 and activity_type='login' and activity_data LIKE 'NOWTV:%'
8%  [ 78 883] mobile gateway:         select count(*) from mpod_audit_log where p = 20131101 and activity_type='login' and activity_data LIKE '%mobile gateway:%'
4%  [ 38 957] ipad_v401:::::50::99::  select  * from mpod_audit_log where p = 20131101 and activity_type='login' and activity_data LIKE '%ipad_v401:::::50::99::%'

Comments ->
  -- (NOWTV::80.238.8.128::::::::::::::::::::) need to lookup IP
Explore ->
select * from mpod_audit_log where p = 20131101 and activity_type='login' and activity_data NOT LIKE '%fixed:%'
select  * from mpod_audit_log where p = 20131101 and activity_type='login' and activity_data NOT LIKE '%fixed:%' and activity_data NOT LIKE 'NOWTV:%'
