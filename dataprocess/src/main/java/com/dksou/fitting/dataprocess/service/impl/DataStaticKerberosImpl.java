package com.dksou.fitting.dataprocess.service.impl;

import com.dksou.fitting.dataprocess.service.DataStaticKerberosService;
import com.dksou.fitting.dataprocess.util.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DataStaticKerberosImpl implements DataStaticKerberosService.Iface{

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";// jdbc驱动路径
    private static String url = "jdbc:hive2://192.168.1.166:10000/default;principal=hive/cdh166@EXAMPLE.COM";// hive库地址+库名

    public static void authKrb5(String user,String krb5Path,String keytabPath) {
        System.out.println("start DataStaticKerberosImpl");
        // 设置jvm启动时krb5的读取路径参数
        //System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        System.setProperty("java.security.krb5.conf",krb5Path);
        // 配置kerberos认证
        Configuration conf = new Configuration();
        conf.setBoolean("hadoop.security.authorization", true);
        conf.set("hadoop.security.authentication", "kerberos");
        // System.out.println(System.getProperty("java.security.krb5.conf"));
        UserGroupInformation.setConfiguration(conf);
        try {
            //UserGroupInformation.loginUserFromKeytab("hive/cdh166@EXAMPLE.COM","/root/hive.keytab");
            UserGroupInformation.loginUserFromKeytab(user,keytabPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // System.out.println("Succeeded in authenticating through Kerberos!");
    }



    @Override
    public double countKerberos(String fun, int fdSum, String spStr, int fdNum, String dirName, String hostIp,
                                String hostPort, String hostName, String hostPassword, String user, String krb5Path,
                                String keytabPath, String principalPath) throws TException {

        // 临时表名
        String tableName = "HiveTmpTabl_" + System.currentTimeMillis();
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;

        long data = 0;



        try {
            authKrb5(user,krb5Path,keytabPath);
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, tableName, fdSum, spStr, dirName);
            data = HiveUtil.selectTab(stmt, tableName, fun, fdNum);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                HiveUtil.deleteTab(stmt, tableName);
                stmt.close();
                conn.close();
                System.out.println("END");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        //return "the " + fun + " for field_" + fdNum + " is " + data;
        return data;
    }

    @Override
    public double countRecordKerberos(String fun, int fdSum, String spStr, int fdNum, String compStr, String whereStr,
                                      String dirName, String hostIp, String hostPort, String hostName,
                                      String hostPassword, String user, String krb5Path, String keytabPath,
                                      String principalPath) throws TException {
        // 临时表名
        String tableName = "HiveTmpTabl_" + System.currentTimeMillis();
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;


        long data = 0;

        try {
            authKrb5(user,krb5Path,keytabPath);
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, tableName, fdSum, spStr, dirName);
            data = HiveUtil.selectTab(stmt, tableName, fun, fdNum,compStr,whereStr);

        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            try {
                HiveUtil.deleteTab(stmt, tableName);
                stmt.close();
                conn.close();
                System.out.println("END");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        //return "the " + fun + " for field_" + fdNum + " is " + data;
        return data;
    }
}
