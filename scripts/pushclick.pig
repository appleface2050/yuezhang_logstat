REGISTER '/home/shanjg/udf.jar';
REGISTER '/home/hadoop/dataware/udf.jar';
REGISTER '/home/hadoop/pig/lib/mysqljdbc.jar';
REGISTER '/home/hadoop/pig/lib/piggybank.jar';
DEFINE logloader com.zy.data.udf.ServiceLogV6Loader;
DEFINE ConvertEngDateStr com.zy.data.udf.ConvertEngDateStr;
logdata = LOAD '$fn' USING logloader();
push= FILTER logdata BY parentbehavior matches '^msgpush_.*$' and  statuscode=='200' and usr!='unknown' and usr matches '\\w+' and usr != 'null';
--gp= GROUP push BY (behavior,behavior_p1);
B = FOREACH push
{
    --usr= DISTINCT usr;
    GENERATE usr, '$date', behavior, behavior_p1;
}
--dump B
STORE B INTO 'out' USING org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://192.168.0.227:3306/logstatV2','analy','analy123','insert into push_click(user_name, ds, behavior, behavior_p1) values(?,?,?,?);','5000');
