package com.dksou.fitting.datasource.service.serviceimpl;

import com.dksou.fitting.datasource.service.DKDBInput;
import com.dksou.fitting.datasource.service.DKDBConf;
import com.dksou.fitting.datasource.service.ResultEntity;
import com.dksou.fitting.datasource.util.SSHImportUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;


public class DKDBInputImpl implements DKDBInput.Iface {

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
    @Override
    public ResultEntity dbFullToHdfs(String connect, String username, String password, String table, String targetDir, String numMappers,
                                     String splitBy, String where, String fileSeparator, String extra, DKDBConf dkdbConf) {
        return SSHImportUtils.rdbmsToHdfs(connect, username, password, table, targetDir, 0, "", "",
                dkdbConf.QUEUE_NAME, numMappers, splitBy, where, fileSeparator, extra, dkdbConf.PRINCIPAL, dkdbConf.KEYTAB);
    }

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
    @Override
    public ResultEntity dbIncrToHdfs(String connect, String username, String password, String table, String targetDir, int writeMode,
                                     String checkColumn, String lastValue, String numMappers, String splitBy, String where, String fileSeparator,
                                     String extra, DKDBConf dkdbConf) {
        return SSHImportUtils.rdbmsToHdfs(connect, username, password, table, targetDir, writeMode, checkColumn, lastValue,
                dkdbConf.QUEUE_NAME, numMappers, splitBy, where, fileSeparator, extra, dkdbConf.PRINCIPAL, dkdbConf.KEYTAB);
    }

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
    @Override
    public ResultEntity dbFullToHive(String connect, String username, String password, String table, String hTable, String numMappers, String splitBy,
                                     String where, String fileSeparator, String extra, DKDBConf dkdbConf) {
        return SSHImportUtils.rdbmsToHive(connect, username, password, table, hTable, 0, "",
                "", dkdbConf.QUEUE_NAME, numMappers, splitBy, where, fileSeparator, extra, dkdbConf.PRINCIPAL, dkdbConf.KEYTAB);
    }

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
    @Override
    public ResultEntity dbIncrToHive(String connect, String username, String password, String table, String hTable, int writeMode, String checkColumn,
                                     String lastValue, String numMappers, String splitBy, String where, String fileSeparator, String extra, DKDBConf dkdbConf) {
        return SSHImportUtils.rdbmsToHive(connect, username, password, table, hTable, writeMode, checkColumn, lastValue,
                dkdbConf.QUEUE_NAME, numMappers, splitBy, where, fileSeparator, extra, dkdbConf.PRINCIPAL, dkdbConf.KEYTAB);
    }

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
    @Override
    public ResultEntity dbFullToHbase(String connect, String username, String password, String table, String hTable, String col, String family, String numMappers,
                                      String splitBy, String where, String extra, DKDBConf dkdbConf) {
        return SSHImportUtils.rdbmsToHbase(connect, username, password, table, hTable, col, family,
                0, "", "", dkdbConf.QUEUE_NAME, numMappers, splitBy, where, extra, dkdbConf.PRINCIPAL, dkdbConf.KEYTAB);
    }


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
    @Override
    public ResultEntity dbIncrToHbase(String connect, String username, String password, String table, String hTable, String col, String family, int writeMode,
                                      String checkColumn, String lastValue, String numMappers, String splitBy, String where, String extra, DKDBConf dkdbConf) {
        return SSHImportUtils.rdbmsToHbase(connect, username, password, table, hTable, col, family,
                writeMode, checkColumn, lastValue, dkdbConf.QUEUE_NAME, numMappers, splitBy, where, extra, dkdbConf.PRINCIPAL, dkdbConf.KEYTAB);
    }
}
