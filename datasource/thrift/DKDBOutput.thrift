namespace java com.dksou.fitting.datasource.service

typedef i32 int // We can use typedef to get pretty names for the types we are using

struct DKDBConf
{
	1:required string	SQOOP_BIN_HOME,
	2:required string	QUEUE_NAME="default",
	3:optional string   PRINCIPAL="",
	4:optional string	KEYTAB="",
}

//封装返回
struct ResultEntity
{
    1:optional i32   status=-1,                   // 状态 0表示成功，-1表示异常
	2:optional string   message="执行成功！"      // 返回的信息
}

service DKDBOutput{
    /**
     * @param connect       JDBC连接字符串
     * @param username      数据库用户名
     * @param password      数据库密码
     * @param table         关系数据库表名，数据导入该表中
     * @param exportDir     导出的HDFS路径
     * @param writeMode     数据导出到数据库的模式 0：INSERT模式导出（将数据转换成Insert语句添加到数据库表中，注意表的一些约束如关键字唯一等）
     *                      1：UPDATE模式导出（updateonly仅更新原有数据，不会插入新数据） 2：UPDATE模式导出（allowinsert，更新原有数据且插入新数据）
     * @param updateKey     根据更新的列，多个列用英文逗号分隔
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param fileSeparator 指定各字段的分隔符，默认为英文逗号
     * @param extra         额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf      dkdb服务的配置项，详见文档
     * @return
     */
	ResultEntity hdfsToRdbms(1:string connect,2:string username,3:string password,4:string table,5:string exportDir,6:i32 writeMode,7:string updateKey 8:string numMappers,9:string fileSeparator,10:string extra,11:DKDBConf dkdbConf),
}
