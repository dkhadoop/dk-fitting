package com.dksou.fitting.datasource.service.serviceimpl;

import com.dksou.fitting.datasource.service.DKDBOutput;
import com.dksou.fitting.datasource.service.DKDBConf;
import com.dksou.fitting.datasource.service.ResultEntity;
import com.dksou.fitting.datasource.util.SSHImportUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;


public class DKDBOutputImpl implements DKDBOutput.Iface {

    /**
     * @param connect       JDBC连接字符串
     * @param username      数据库用户名
     * @param password      数据库密码
     * @param table         关系数据库表名，数据导入该表中
     * @param exportDir     导出的HDFS路径
     * @param writeMode     数据导入到数据库的模式 0：INSERT模式导入（将数据转换成Insert语句添加到数据库表中，注意表的一些约束如关键字唯一等）
     *                      1：UPDATE模式导出（updateonly仅更新原有数据，不会插入新数据） 2：UPDATE模式导出（allowinsert，更新原有数据且插入新数据）
     * @param updateKey     根据更新的列，多个列用英文逗号分隔
     * @param numMappers    启动的map来并行导入数据，默认是4个，最好不要将数字设置为高于集群的节点数
     * @param fileSeparator 指定各字段的分隔符，默认为英文逗号
     * @param extra         额外的合法参数，参考sqoop官网（格式：--xxx --xxx）
     * @param dkdbConf      dkdb服务的配置项，详见文档
     * @return
     */
    @Override
    public ResultEntity hdfsToRdbms(String connect, String username, String password, String table, String exportDir, int writeMode,
                                    String updateKey, String numMappers, String fileSeparator, String extra, DKDBConf dkdbConf) {
        return SSHImportUtils.hdfsToRdbms(connect, username, password, table,
                exportDir, dkdbConf.QUEUE_NAME, writeMode, updateKey, numMappers, fileSeparator, extra, dkdbConf.PRINCIPAL, dkdbConf.KEYTAB);
    }
}
