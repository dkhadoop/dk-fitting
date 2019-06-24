namespace java com.dksou.fitting.datasource.service

typedef i32 int // We can use typedef to get pretty names for the types we are using

//1. 提供各种sqoop 支持的数据库到hdfs 、hbase、hive的方法。名称自定义，参数自定义。
struct FileData
{
    1:required string   name,  // 文件名称
    2:required binary   buff,  // 文件数据
	3:required i32		status,// 返回的状态：0表示文件下载成功，-1表示文件下载异常
	4:required string   message //返回的信息
}
struct DKFILEConf
{
	1:optional string   KRB5_CONF="",//配置使用服务器krb5.conf文件的地址
	2:optional string   PRINCIPAL="",//配置主体名称
	3:optional string	KEYTAB="",//配置使用服务器keytab文件的地址
}
//封装返回
struct ResultEntity
{
    1:optional i32   status=-1,                   // 状态 0表示成功，-1表示异常
	2:optional string   message="执行成功！"                // 返回的信息
}
service DKFileOutput{
    /**
     * @param exportDir  hdfs下载的文件名（路径+文件名全称）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
	FileData hdfsToFileByBinary(1:string exportDir,2:DKFILEConf dkFileConf),
    /**
     * @param filePath   为本地文件目录名
     * @param exportDir  要从hdfs导出的目录
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
	ResultEntity hdfsToFiles(1:string filePath,2:string exportDir,3:DKFILEConf dkFileConf),
}