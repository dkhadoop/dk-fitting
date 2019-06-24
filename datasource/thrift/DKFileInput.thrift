namespace java com.dksou.fitting.datasource.service

typedef i32 int // We can use typedef to get pretty names for the types we are using
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
service DKFileInput{
    /**
     * @param fileName   文件名
     * @param fileData   文件内容
     * @param dirName    输出的hdfs路径（不包括文件名）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
	ResultEntity fileToHdfsByBinary(1:string fileName,2:binary fileData,3:string dirName,4:DKFILEConf dkFileConf),
    /**
     * @param filePath   单个文件的绝对路径（包含文件名）
     * @param dirName    输出的hdfs路径（不包含文件名）
     * @param fileLength 文件长度限制（K为单位。）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
	ResultEntity fileToHdfs(1:string filePath, 2:string dirName,3:i32 fileLength,4:DKFILEConf dkFileConf),
    /**
     * @param filePath   多文件上传，参数为文件的父路径
     * @param dirName    输出到hdfs上的路径
     * @param fileLength 文件长度限制（K为单位。）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
	ResultEntity filesToHdfs(1:string filePath, 2:string dirName,3:i32 fileLength,4:DKFILEConf dkFileConf),
}