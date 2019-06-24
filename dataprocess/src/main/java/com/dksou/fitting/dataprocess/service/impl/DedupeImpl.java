package com.dksou.fitting.dataprocess.service.impl;


import com.dksou.fitting.dataprocess.service.DedupeService;
import com.dksou.fitting.dataprocess.util.HiveUtil;
import org.apache.thrift.TException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;



public class DedupeImpl implements DedupeService.Iface {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * 该方法可筛选出不同的数据或字段
     * @param spStr
     * @param fdNum
     * @param srcDirName
     * @param dstDirName
     * @param hostIp
     * @param hostPort
     * @param hostName
     * @param hostPassword
     * @throws Exception
     */
    public void dedup(String spStr, String fdNum, String srcDirName, String dstDirName,
                      String hostIp, String hostPort,
                      String hostName, String hostPassword) throws TException {

        String driverName = "org.apache.hive.jdbc.HiveDriver";
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
            HiveUtil.dedupe(stmt, dstDirName, srcTable, dstTable, spStr,fdNum);
            HiveUtil.toExTable(stmt, dstTable);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                HiveUtil.deleteTab(stmt, srcTable);
                HiveUtil.deleteTab(stmt, dstTable);
                stmt.close();
                conn.close();
                System.out.print("jieshu");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }






}
