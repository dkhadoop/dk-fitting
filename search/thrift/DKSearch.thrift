namespace java com.dksou.fitting.search.SearchService

typedef i32 int // We can use typedef to get pretty names for the types we are using
service SearchService{



/**
        *条件查询
        * @param hostIps ES集群的IP
        * @param clusterName ES集群的名称
        * @param indexName ES的索引名称
        * @param TypeName ES的类型名称
        * @param port ES的连接端口
        * @param start 记录偏移 , null-默认为0
        * @param size 记录数 , null-默认为10
        * sentence 查询字段
        */

        map<string,string> ConditionalQuery( 1:string hostIps,2:string clusterName,3:string indexName,4:string typeName,5:int port,6:int start,7:int size,8:string sentence);

/**
        *模糊查询，通配符查询
        * @param hostIps ES集群的IP
        * @param clusterName ES集群的名称
        * @param indexName ES的索引名称
        * @param TypeName ES的类型名称
        * @param port ES的连接端口
        * @param start 记录偏移 , null-默认为0
        * @param size 记录数 , null-默认为10
        * sentence 查询字段
        * field 字段名
        */

        map<string,string> FuzzyQuery( 1:string hostIps,2:string clusterName,3:string indexName,4:string typeName,5:int port,6:int start,7:int size,8:string sentence,9:string field);
/**
        * 过滤查询
        * @param hostIps ES集群的IP
        * @param clusterName ES集群的名称
        * @param indexName ES的索引名称
        * @param TypeName ES的类型名称
        * @param port ES的连接端口
        * @param start 记录偏移 , null-默认为0
        * @param size 记录数 , null-默认为10
        * field 查询字段
        * gte 过滤条件(大于等于)
        * lte 过滤条件(小于等于)
        */

        map<string,string> FileterQuery( 1:string hostIps,2:string clusterName,3:string indexName,4:string typeName,5:int port,6:int start,7:int size,8:string field,9:string gte,10:string lte);


        /**
        * 聚合查询
         * @param hostIp 要连接搜索主机的ip地址
         * @param port 搜索引擎的端口号(默认9300)
         * @param clusterName 集群名称
         * @param indexName 搜索引擎的索引名称（自定义）
         * @param typeName 搜索引擎的索引类型名称（自定义）
         * @param aggFdName 需要聚合的字段
         * @param aggType 聚合的类型,格式DKSearch.TYPE,如求平均数时传入DKSearch.AVG
         *
         */
        map<string,string>  StatsAggregation(1:string hostIp, 2:int port, 3:string clusterName, 4:string indexName, 5:string typeName, 6:string aggFdName, 7:string aggType);
 		
 
 /**
        * 在搜索中添加聚合
         * @param hostIp 要连接搜索主机的ip地址
         * @param port 搜索引擎的端口号(默认9300)
         * @param clusterName 集群名称
         * @param indexName 搜索引擎的索引名称（自定义）
         * @param typeName 搜索引擎的索引类型名称（自定义）
         * @param aggFdName 需要聚合的字段
         * @param aggType 聚合的类型,格式DKSearch.TYPE,如求平均数时传入DKSearch.AVG
         *
         */
         map<string,string> PercentilesAggregation(1:string hostIp, 2:int port, 3:string clusterName, 4:string indexName, 5:string typeName, 6:string aggFdName, 7:string aggType);

/**
        * 根据字段值项分组聚合
         * @param hostIp 要连接搜索主机的ip地址
         * @param port 搜索引擎的端口号(默认9300)
         * @param clusterName 集群名称
         * @param indexName 搜索引擎的索引名称（自定义）
         * @param typeName 搜索引擎的索引类型名称（自定义）
         * @param aggFdName 需要聚合的字段
         * @param aggType 聚合的类型,格式DKSearch.TYPE,如求平均数时传入DKSearch.AVG
         *
         */
         map<string,string> TermsAggregation(1:string hostIp, 2:int port, 3:string clusterName, 4:string indexName, 5:string typeName, 6:string aggFdName);

/**
        * 多条件组合与查询 
         * @param hostIp 要连接搜索主机的ip地址
         * @param port 搜索引擎的端口号(默认9300)
         * @param clusterName 集群名称
         * @param indexName 搜索引擎的索引名称（自定义）
         * @param typeName 搜索引擎的索引类型名称（自定义）
         * @param filed 字段名
         * @param filedName 查询短语 
         *
         */
         map<string,string> MustSearch(1:string hostIp, 2:int port, 3:string clusterName, 4:string indexName, 5:string typeName, 6:string filed, 7:string filedName,8:int start,9:int size);

/**
        * 多条件或查询
         * @param hostIp 要连接搜索主机的ip地址
         * @param port 搜索引擎的端口号(默认9300)
         * @param clusterName 集群名称
         * @param indexName 搜索引擎的索引名称（自定义）
         * @param typeName 搜索引擎的索引类型名称（自定义）
         * @param field 字段名
         * @param filedName 查询短语
         *
         */
         map<string,string> ShouldSearch(1:string hostIp, 2:int port, 3:string clusterName, 4:string indexName, 5:string typeName, 6:string filed, 7:string filedName,8:int start,9:int size);

 }