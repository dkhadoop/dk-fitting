package com.dksou.fitting.dataprocess.service.impl;

import com.dksou.fitting.dataprocess.service.DataStaticService;
import com.dksou.fitting.dataprocess.util.HiveUtil;
import org.apache.thrift.TException;

import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.SQLException;
import java.sql.Statement;

public class DataStaticImpl implements DataStaticService.Iface {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * 该方法可对某字段取最大值、最小值、求和、计算平均值
     * @param fun 功能avg,min,max,sum
     * @param fdSum
     * @param spStr
     * @param fdNum
     * @param dirName
     * @param hostIp
     * @param hostPort
     * @param hostName
     * @param hostPassword
     * @return
     */
    public double count(String fun, int fdSum, String spStr, int fdNum, String dirName, String hostIp,
                        String hostPort,
                        String hostName, String hostPassword) throws TException {
        // 临时表名
        String tableName = "HiveTmpTabl_" + System.currentTimeMillis();
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default";
        long data = 0;

        Statement stmt = null;
        Connection conn = null;;



        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, tableName, fdSum, spStr, dirName);
            data = HiveUtil.selectTab(stmt, tableName, fun, fdNum);
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            try {
                HiveUtil.deleteTab(stmt, tableName);
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        //return "the " + fun + " for field_" + fdNum + " is " + data;
        return data;
    }

    /**
     * 该方法可计算某字段符合某条件的记录数
     * @param fun 功能count
     * @param fdSum
     * @param spStr
     * @param fdNum
     * @param compStr 比较符号，>, <, >=, <=, =,!=用法："'>='"
     * @param whereStr
     * @param dirName
     * @param hostIp
     * @param hostPort
     * @param hostName
     * @param hostPassword
     * @return
     * @throws Exception
     */
    public double countRecord(String fun, int fdSum, String spStr, int fdNum, String compStr,
                              String whereStr, String dirName,String hostIp, String hostPort,
                              String hostName, String hostPassword) throws TException {
        // 临时表名
        String tableName = "HiveTmpTabl_" + System.currentTimeMillis();
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default";

        Statement stmt = null;
        Connection conn = null;

        long data = 0;

        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, tableName, fdSum, spStr, dirName);
            data = HiveUtil.selectTab(stmt, tableName, fun, fdNum,compStr,whereStr);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                HiveUtil.deleteTab(stmt, tableName);
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        //return "the " + fun + " for field_" + fdNum + " is " + data;
        return data;
    }





}
