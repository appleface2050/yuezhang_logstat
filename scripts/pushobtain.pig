REGISTER '/home/shanjg/udf.jar';
REGISTER '/home/hadoop/dataware/udf.jar';
REGISTER '/home/hadoop/pig/lib/mysqljdbc.jar';
REGISTER '/home/hadoop/pig/lib/piggybank.jar';
DEFINE logloader com.zy.data.udf.ServiceLogV6Loader;
DEFINE ConvertEngDateStr com.zy.data.udf.ConvertEngDateStr;
DEFINE SafeInt com.zy.data.udf.SafeInt;
logdata = LOAD '$fn' USING logloader();
push= FILTER logdata BY key =='4U5' AND act=='UC2BusiAll' AND action=='getAdviceInfo' AND statuscode=='200' AND SafeInt(contentlength)>70;
gp= GROUP push BY channel;--SUBSTRING(ConvertEngDateStr(datetime),10,13);
B = FOREACH gp
{
    usr= FOREACH push GENERATE user;
    du= DISTINCT usr;
    GENERATE '$date', COUNT(push),COUNT(du), group;
}

--DUMP B
STORE B INTO 'out' USING org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://192.168.0.227:3306/logstatV2','analy','analy123','insert into push_obtain(ds, pv, 
uv, channel) values(?,?,?,?);','5000');
