namespace java com.dksou.fitting.sqlutils.service

typedef i32 int // We can use typedef to get pretty names for the types we are using

struct DKSQLConf
{                
	1:optional string   KRB5_CONF="",//krb5.conf文件路径配置
	2:optional string   PRINCIPAL="",//principal主体名称配置
	3:optional string	KEYTAB="",//keytab文件路径配置
}
struct ResultEntity
{
    1:optional i32   status=-1,                   // 状态 0表示成功，-1表示异常
	2:optional string message="执行成功！" ,     // 返回的信息
	3:optional string result
}

service DKSQLEngine{	
    /**
     * @param hostIp    Hive连接ip地址
     * @param port      Hive连接端口
     * @param username  用户名
     * @param password  密码
     * @param database  数据库
     * @param sql       要执行的sql语句
     * @param queueName 指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置）
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return
     */
	ResultEntity executeHQL(1:string hostIp,2:string port,3:string username,4:string password,5:string database,6:string sql,7:string queueName,8:DKSQLConf dkSqlConf),
    /**
     * @param hostIp    Impala连接ip地址
     * @param port      Impala连接端口
     * @param database  要连接的数据库
     * @param sql       需要执行的sql
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return String
     */
	ResultEntity excuteISQL(1:string hostIp,2:string port,3:string database,4:string sql,5:DKSQLConf dkSqlConf),
    /**
     * @param hostIp    hive 的连接IP地址
     * @param port      hive的连接端口
     * @param username  用户名
     * @param password  密码
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param orderBy   排序字段
     * @param startRow  开始行
     * @param endRow    结束行
     * @param queueName 指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置）
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return String
     */
	ResultEntity excuteQueryHQL(1:string hostIp,2:string port,3:string username,4:string password,5:string database,6:string sql,7:string orderBy,8:i32 startRow,9:i32 endRow,10:string queueName,11:DKSQLConf dkSqlConf),
   /**
     * @param hostIp    Impala的连接IP地址
     * @param port      Impala连接端口
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return String
     */
	ResultEntity excuteQueryISQL(1:string hostIp,2:string port,3:string database,4:string sql,5:DKSQLConf dkSqlConf),
    /**
     * @param hostIp    hive的连接IP地址
     * @param port      hive的连接端口
     * @param username  用户名
     * @param password  密码
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param queueName 指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置）
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return String
     */
	ResultEntity countHQL(1:string hostIp,2:string port,3:string username,4:string password,5:string database,6:string sql,7:string queueName,8:DKSQLConf dkSqlConf),
    /**
     * @param hostIp    Impala的连接IP地址
     * @param port      Impala的连接端口
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return String
     */
	ResultEntity countIQL(1:string hostIp,2:string port,3:string database,4:string sql,5:DKSQLConf dkSqlConf),
	
}


//IF NOT EXISTS
//分页查询
//select * from  (select row_number() over (order by id) as rnum, tb.* from (select * from customer where id>2) as tb)t where rnum between '1' and '2';

