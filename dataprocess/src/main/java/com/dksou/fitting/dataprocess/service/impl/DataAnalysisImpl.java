package com.dksou.fitting.dataprocess.service.impl;

import com.dksou.fitting.dataprocess.service.DataAnalysisService;
import com.dksou.fitting.dataprocess.util.HiveUtil;
import org.apache.thrift.TException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DataAnalysisImpl implements DataAnalysisService.Iface {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * 该方法可用于对数据条件筛选分析或分组统计分析。
     * @param spStr
     * @param fdSum
     * @param whereStr
     * @param groupStr
     * @param srcDirName
     * @param dstDirName
     * @param hostIp
     * @param hostPort
     * @param hostName
     * @param hostPassword
     * @throws Exception
     *
     */
    public void analyse(String spStr, int fdSum, String whereStr, String groupStr, String srcDirName,
                        String dstDirName, String hostIp, String hostPort, String hostName,
                        String hostPassword) throws TException {

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
            HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);
            HiveUtil.groupAnalyse(stmt, dstDirName, srcTable, dstTable, spStr,fdSum,whereStr,groupStr);
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

    /**
     * 该方法可分析某两种物品同时出现的频率
     * @param spStr
     * @param fdSum
     * @param pNum
     * @param oNum
     * @param whereStr
     * @param srcDirName
     * @param dstDirName
     * @param hostIp
     * @param hostPort
     * @param hostName
     * @param hostPassword
     * @throws Exception
     */
    public void apriori2(String spStr, int fdSum, String pNum, String oNum, String whereStr,
                         String srcDirName, String dstDirName,
                         String hostIp, String hostPort, String hostName, String hostPassword) throws TException {


        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String midTable = srcTable + "_mid";
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";

        Statement stmt = null;
        Connection conn = null;

        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);
            HiveUtil.apriori2(stmt, dstDirName, srcTable,midTable, dstTable, spStr,fdSum,pNum,oNum,whereStr);
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



    /**
     * 该方法可分析某三种物品同时出现的频率。
     * @param spStr
     * @param fdSum
     * @param pNum
     * @param oNum
     * @param whereStr
     * @param srcDirName
     * @param dstDirName
     * @param hostIp
     * @param hostPort
     * @param hostName
     * @param hostPassword
     * @throws Exception
     */
    public void apriori3(String spStr, int fdSum, String pNum, String oNum, String whereStr,
                         String srcDirName, String dstDirName,String hostIp, String hostPort, String hostName,
                         String hostPassword) throws TException {

        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String midTable = srcTable + "_mid";
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";

        Statement stmt = null;
        Connection conn = null;

        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);
            HiveUtil.apriori3(stmt, dstDirName, srcTable,midTable, dstTable, spStr,fdSum,pNum,oNum,whereStr);
            HiveUtil.toExTable(stmt, dstTable);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                HiveUtil.deleteTab(stmt, srcTable);
                HiveUtil.deleteTab(stmt, midTable);
                HiveUtil.deleteTab(stmt, dstTable);
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }




}
