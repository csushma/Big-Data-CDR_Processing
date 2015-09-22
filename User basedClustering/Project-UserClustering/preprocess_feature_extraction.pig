//step1: parse and format raw data
A = load '/bxx140230/tip.txt' as (line:chararray);
B = foreach A generate REPLACE(line, '"', '');  
store B into '/bxx140230/tip1';
B = load '/bxx140230/tip1/part-m-00000'


//step2: preprocess to get user fetures as (uid, callnum, distinctdaynum, maxTimePeriod, totalDuration)
REGISTER /people/cs/b/bxx140230/format_time.jar;
B = load '/bxx140230/tip1/part-m-00000' using PigStorage(',') as (id:chararray, uid:chararray, date:chararray, time:chararray, duration:double, lantitude:chararray, longitude:chararray);
C = foreach B generate uid as(uid), date as(date), FORMAT_TIME(time) as(time), duration as (duration);

D = group C by uid;
Dcnt = foreach D generate group, COUNT($1) as (tCnt);
Dsum = foreach D generate group, SUM($1.duration) as (sumDur);

DdateCnt = foreach D {
	D1 = C.date;
	D2 = distinct D1;
	generate group, COUNT(D2) as (distDate);
}

F = foreach C generate uid as (uid), time as(time);
F1 = group F by (uid,time);
G = foreach F1 generate group, COUNT($1) as (cnt);
M = foreach G generate $0.uid, $0.time, $1 as (cnt);
H = group M by uid;
I = foreach H {
	X = order M by cnt desc;
	Y = limit X 1;
	generate group,Y;
}
DmaxTime = foreach I generate $0, flatten($1.$1) as (maxTime);

N = join Dcnt by $0, DdateCnt by $0, DmaxTime by $0, Dsum by $0;
X = foreach N generate Dcnt::group, Dcnt::tCnt, DdateCnt::distDate, DmaxTime::maxTime, Dsum::sumDur;
store X into '/bxx140230/output/14_out';
