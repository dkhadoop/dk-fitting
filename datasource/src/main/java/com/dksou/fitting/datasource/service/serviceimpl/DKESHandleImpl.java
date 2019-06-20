package com.dksou.fitting.datasource.service.serviceimpl;

import com.dksou.fitting.datasource.service.DKESHandle;
import com.dksou.fitting.datasource.util.SSHImportUtils;
import org.apache.thrift.TException;

public class DKESHandleImpl implements DKESHandle.Iface {
    /**
     * @param connect       JDBC连接字符串
     * @param username      数据库用户名
     * @param password      数据库密码
     * @param table         关系数据库表名，数据从该表中获取
     * @param esIpAndPort   连接elasticsearch的地址及端口（eg:192.168.1.126：9200，端口默认为9200
     * @param esClusterName 为连接elasticsearch的集群名称
     * @param esIndexName   为要导出到elasticsearch的索引名称
     * @param esTypeName    为要导出到的类型名称
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param where         从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param queueName     指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置），默认default
     */
    @Override
    public void dbToEs(String connect, String username, String password, String table, String esIpAndPort,
                       String esClusterName, String esIndexName, String esTypeName, String numMappers, String where, String queueName) {
        SSHImportUtils.rdbmsToEs(connect, username, password, table, esIpAndPort, esClusterName, esIndexName, esTypeName, numMappers, where, queueName);
    }

    /**
     * @param esIpAndPort   连接elasticsearch的地址及端口（eg:192.168.1.126：9200，端口默认为9200
     * @param esClusterName 为连接elasticsearch的集群名称
     * @param esIndexName   为要导出到elasticsearch的索引名称
     * @param esTypeName    为要导出到的类型名称
     * @param exportDir     要从hdfs导出的目录
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param fileSeparator 指定各字段的分隔符，默认为英文逗号
     * @param queueName     指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置），默认default
     */
    @Override
    public void hdfsToEs(String esIpAndPort, String esClusterName, String esIndexName, String esTypeName, String exportDir, String
            numMappers, String fileSeparator, String queueName) {
        SSHImportUtils.hdfsToEs(esIpAndPort, esClusterName, esIndexName, esTypeName, exportDir, numMappers, fileSeparator, queueName);
    }

    /**
     * @param connect       JDBC连接字符串
     * @param table         关系数据库表名，数据从该表中获取
     * @param esIpAndPort   连接elasticsearch的地址及端口（eg:192.168.1.126：9200，端口默认为9200
     * @param esClusterName 为连接elasticsearch的集群名称
     * @param esIndexName   为要导出到elasticsearch的索引名称
     * @param esTypeName    为要导出到的类型名称
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param where         从关系数据库导入数据时的查询条件，示例：–where “id = 2”
     * @param queueName     指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置），默认default
     */
    @Override
    public void sqlServerToEs(String connect, String table, String esIpAndPort,
                              String esClusterName, String esIndexName, String esTypeName, String numMappers, String where,
                              String queueName) {
        SSHImportUtils.sqlServerToEs(connect, table, esIpAndPort, esClusterName, esIndexName, esTypeName, numMappers, where, queueName);
    }
}
