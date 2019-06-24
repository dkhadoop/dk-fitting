namespace java com.dksou.fitting.SearchIOService

typedef i32 int // We can use typedef to get pretty names for the types we are using
 struct FileData
		{
	    1:required string   name,                   // 文件名字
	    2:required binary   buff,                   // 文件数据
		}
service DKSearchInput{



/**
        *获取hdfs中的数据到ES
        * @param fs hdfs的客户端路径
        * @param dirName hdfs的目录名称
        * @param hostIps ES集群的IP
        * @param clusterName ES集群的名称
        * @param indexName ES的索引名称
        * @param TypeName ES的类型名称
        * @param port ES的连接端口
        * @param length 设置获取数据的多少，length * 1024
        */

        string nosql2ES(1:string fs,2:string dirName,3:string hostIps,4:string clusterName,5:string indexName,6:string typeName,7:int port,8:int length);



        /**
        * 服务端导入字节流数组
        * @param fileType  文件类型
        * @param filePath  文件路径
        * @param hostIps   ES的IP
        * @param clusterName ES的集群名称
        * @param indexName  ES的索引名称
        * @param typeName  ES的类型名称
        * @param port    连接ES的端口
        * @param length  设置获取数据的多少，length * 1024
        */
    string file2ES(1:int fileType,2:string filePath,3:string hostIps,4:string clusterName,5:string indexName,6:string typeName,7:int port,8:int length);
    /**
        * 客户端导入二进制流
        * @param fileName  文件名称
        * @param fileDir  文件目录
        * @param hostIps   ES的IP
        * @param clusterName ES的集群名称
        * @param indexName  ES的索引名称
        * @param typeName  ES的类型名称
        * @param port    连接ES的端口
        * @param fileData  二进制字节流
        */


    string file2ESByBinary(1:string fileName,2:string fileDir,3:string hostIps,4:string clusterName,5:string indexName,6:string typeName,7:int port,8:FileData fileData,9:int fileType);
     /**
        * excel数据导入
        * @param fileName  文件名称
        * @param fileDir  文件目录
        * @param hostIps   ES的IP
        * @param clusterName ES的集群名称
        * @param indexName  ES的索引名称
        * @param typeName  ES的类型名称
        * @param port    连接ES的端口
        * @param fileData  二进制字节流
        */
    string file2ESExcel(1:string fileName,2:string fileDir,3:string hostIps,4:string clusterName,5:string indexName,6:string typeName,7:int port);

 }