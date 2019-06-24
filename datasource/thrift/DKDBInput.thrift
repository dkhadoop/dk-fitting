namespace java com.dksou.fitting.datasource.service

typedef i32 int // We can use typedef to get pretty names for the types we are using

struct DKDBConf
{
	1:optional string	QUEUE_NAME="default",
	2:optional string   PRINCIPAL="",
	3:optional string	KEYTAB="",
}
//封装返回
struct ResultEntity
{
    1:optional i32   status=-1,                   // 状态 0表示成功，-1表示异常
	2:optional string   message="任务执行成功！"                // 返回的信息
}
service DKDBInput{
    /**
     * @param connect       JDBC连接字符串
     * @param username      数据库用户名
     * @param password      数据库密码
     * @param table         关系数据库表名，数据从该表中获取
     * @param targetDir     指定hdfs路径,数据导入至该路径下
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param splitBy       表的列名，用来切分工作单元，一般后面跟主键ID，和numMappers参数结合使用，详见文档
     * @param where         从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param fileSeparator 指定各字段的分隔符，默认为英文逗号
     * @param extra         额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf      dkdb服务的配置项，详见文档
     * @return
     */
	ResultEntity dbFullToHdfs(1:string connect,2:string username,3:string password,4:string table,5:string targetDir,6:string numMappers,7:string splitBy,8:string where,9:string fileSeparator,10:string extra,11:DKDBConf dkdbConf),
    /**
     * @param connect       JDBC连接字符串
     * @param username      数据库用户名
     * @param password      数据库密码
     * @param table         关系数据库表名，数据从该表中获取
     * @param targetDir     指定hdfs路径,数据导入至该路径下
     * @param writeMode     写入模式：1表示Append模式增量导入，需设置一个自增主键来作为检查字段 2表示LastModified的append增量导入 备注：详细说明见文档
     * @param checkColumn   增量导入模式下指定要检查的列
     * @param lastValue     指定前一个导入中的检查列的最大值
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param splitBy       表的列名，用来切分工作单元，一般后面跟主键ID，和numMappers参数结合使用，详见文档
     * @param where         从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param fileSeparator 指定各字段的分隔符，默认为英文逗号
     * @param extra         额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf      dkdb服务的配置项，详见文档
     * @return
     */
	ResultEntity dbIncrToHdfs(1:string connect,2:string username,3:string password,4:string table,5:string targetDir,6:i32 writeMode,7:string checkColumn,8:string lastValue,9:string numMappers,10:string splitBy,11:string where,
	12:string fileSeparator,13:string extra,14:DKDBConf dkdbConf),
    /**
     * @param connect       JDBC连接字符串
     * @param username      数据库用户名
     * @param password      数据库密码
     * @param table         关系数据库表名，数据从该表中获取
     * @param hTable        指定导入到Hive中的表名
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param splitBy       表的列名，用来切分工作单元，一般后面跟主键ID，和numMappers参数结合使用，详见文档
     * @param where         从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param fileSeparator 指定各字段的分隔符，默认为英文逗号
     * @param extra         额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf      dkdb服务的配置项，详见文档
     * @return
     */
	ResultEntity dbFullToHive(1:string connect,2:string username,3:string password,4:string table,5:string hTable,6:string numMappers,7:string splitBy,8:string where,9:string fileSeparator,10:string extra,11:DKDBConf dkdbConf),

    /**
     * @param connect       JDBC连接字符串
     * @param username      数据库用户名
     * @param password      数据库密码
     * @param table         关系数据库表名，数据从该表中获取
     * @param hTable        指定导入到Hive中的表名
     * @param writeMode     写入模式：1表示Append模式增量导入，需设置一个自增主键来作为检查字段 2表示LastModified增量导入 备注：详细说明见文档
     * @param checkColumn   增量导入模式下指定要检查的列
     * @param lastValue     指定前一个导入中的检查列的最大值
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param splitBy       表的列名，用来切分工作单元，一般后面跟主键ID，和numMappers参数结合使用，详见文档
     * @param where         从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param fileSeparator 指定各字段的分隔符，默认为英文逗号
     * @param extra         额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf      dkdb服务的配置项，详见文档
     * @return
     */
	ResultEntity dbIncrToHive(1:string connect,2:string username,3:string password,4:string table,5:string hTable,6:i32 writeMode,7:string checkColumn,8:string lastValue,9:string numMappers,10:string splitBy,11:string where,
	12:string fileSeparator,13:string extra,14:DKDBConf dkdbConf),
    /**
     * @param connect    JDBC连接字符串
     * @param username   数据库用户名
     * @param password   数据库密码
     * @param table      关系数据库表名，数据从该表中获取
     * @param hTable     指定导入到Hbase中的表名
     * @param col        指定数据表的哪一列作为rowkey，建议主键
     * @param family     Hbase数据表列族名
     * @param numMappers 启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param splitBy    表的列名，用来切分工作单元，一般后面跟主键ID，和numMappers参数结合使用，详见文档
     * @param where      从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param extra      额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf   dkdb服务的配置项，详见文档
     * @return
     */
	ResultEntity dbFullToHbase(1:string connect,2:string username,3:string password,4:string table,5:string hTable,6:string col,7:string family,8:string numMappers,9:string splitBy,10:string where,11:string extra,12:DKDBConf dkdbConf),
    /**
     * @param connect     JDBC连接字符串
     * @param username    数据库用户名
     * @param password    数据库密码
     * @param table       关系数据库表名，数据从该表中获取
     * @param hTable      指定导入到Hbase中的表名
     * @param col         指定数据表的哪一列作为rowkey，建议主键
     * @param family      Hbase数据表列族名
     * @param writeMode   写入模式：1表示Append模式增量导入，需设置一个自增主键来作为检查字段 2表示LastModified的append增量导入 备注：详细说明见文档
     * @param checkColumn 增量导入模式下指定要检查的列
     * @param lastValue   指定前一个导入中的检查列的最大值
     * @param numMappers  启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param splitBy     表的列名，用来切分工作单元，一般后面跟主键ID，和numMappers参数结合使用，详见文档
     * @param where       从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param extra       额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf    dkdb服务的配置项，详见文档
     * @return
     */
	ResultEntity dbIncrToHbase(1:string connect,2:string username,3:string password,4:string table,5:string hTable,6:string col,7:string family,8:i32 writeMode,9:string checkColumn,10:string lastValue,11:string numMappers,12:string splitBy,
	13:string where,14:string extra,15:DKDBConf dkdbConf),
	
}