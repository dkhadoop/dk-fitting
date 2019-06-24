package com.dksou.fitting.dataprocess.service.impl;

import com.dksou.fitting.dataprocess.service.FormatFieldService;
import com.dksou.fitting.dataprocess.util.HiveUtil;
import org.apache.thrift.TException;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class FormatFieldImpl implements FormatFieldService.Iface {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    /**
     * 调用此方法可以按关键字过滤出想要的字段
     * @param spStr 分隔符号
     * @param fdSum 字段数量
     * @param fdNum 字段序号
     * @param regExStr 字段中包含该字符的记录将被剔除（a,b,c），与字段序号相对应，多个字段时每个字段都符合该条件的记录将被剔除
     * @param srcDirName 源目录名
     * @param dstDirName 输出目录名
     * @param hostIp 要连接hiveserver主机的ip地址
     * @param hostPort hiveserver的端口，默认10000
     * @param hostName 要连接主机的用户名
     * @param hostPassword 要连接主机的密码
     * @throws Exception
     */
    public void formatField(String spStr, int fdSum, String fdNum, String regExStr, String srcDirName,
                            String dstDirName, String hostIp, String hostPort,
                            String hostName, String hostPassword) throws TException {

        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";

        Statement stmt = null;
        Connection conn = null;



        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, srcTable, srcDirName);
            HiveUtil.formatField(stmt, dstDirName, srcTable, dstTable, spStr,fdSum, fdNum, regExStr);
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
