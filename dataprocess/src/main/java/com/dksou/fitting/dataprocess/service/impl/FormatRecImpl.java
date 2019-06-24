package com.dksou.fitting.dataprocess.service.impl;

import com.dksou.fitting.dataprocess.service.FormatRecService;
import com.dksou.fitting.dataprocess.util.HiveUtil;
import org.apache.thrift.TException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class FormatRecImpl implements FormatRecService.Iface {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    /**
     * 调用此方法可以将不合法的记录去除掉
     * @param spStr 分隔符号
     * @param fdSum 字段数量
     * @param srcDirName 源目录名
     * @param dstDirName 输出目录名，输出目录如果存在将会覆盖
     * @param hostIp 要连接hiveserver主机的ip地址
     * @param hostPort hiveserver的端口，默认10000
     * @param hostName  要连接主机的用户名
     * @param hostPassword 要连接主机的密码
     * @throws Exception
     */
    public void formatRec(String spStr, int fdSum, String srcDirName, String dstDirName, String hostIp,
                          String hostPort, String hostName, String hostPassword) throws TException {
        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";

        Statement stmt = null;
        Connection conn = null;

        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            HiveUtil.createTab(stmt, srcTable, srcDirName);
            HiveUtil.formatRec(stmt, dstDirName, srcTable, dstTable, spStr,fdSum);
            HiveUtil.toExTable(stmt, dstTable);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                HiveUtil.deleteTab(stmt, srcTable);
                HiveUtil.deleteTab(stmt, dstTable);
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }







}
