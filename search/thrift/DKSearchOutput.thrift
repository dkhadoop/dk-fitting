namespace java com.dksou.fitting.SearchIOService

typedef i32 int // We can use typedef to get pretty names for the types we are using
service DKSearchOutput{





     /**
	 *获取ES数据总和
     * @param hostIps ES的ip地址
     * @param clusterName ES的集群名称
     * @param indexName ES的索引名称
     * @param typeName ES的类型名称
     * @param port ES的端口号
     */
    i64 getESSum(1:string hostIps,2:string clusterName,3:string indexName,4:string typeName,5:int port);




    /**
     * 导出ES数据.txt类型
     * @param hostIps ES的ip地址
     * @param clusterName ES的集群名称
     * @param indexName ES的索引名称
     * @param typeName ES的类型名称
     * @param port ES的端口号
     * @param start 从哪儿开始
     * @param size 到哪儿结束
     */
        string ES2Txt(1:string hostIps,2:string clusterName,3:string indexName,4:string typeName,5:int port,6:int start,7:int size);

     /**
      * 导出ES数据 .xls类型
      * @param hostIps ES的ip地址
      * @param clusterName ES的集群名称
      * @param indexName ES的索引名称
      * @param typeName ES的类型名称
      * @param port ES的端口号
      * @param  start 从哪儿开始
      * @param size 到哪儿结束
      */
         string ES2XLS(1:string hostIps,2:string clusterName,3:string indexName,4:string typeName,5:int port,6:int  start,7:int size);


        
}