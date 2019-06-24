package com.dksou.fitting.dataprocess.service.impl;

import com.dksou.fitting.dataprocess.service.DataCleanKerberosService;
import com.dksou.fitting.dataprocess.util.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DataCleanKerberosImpl implements DataCleanKerberosService.Iface {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    public static void authKrb5(String user,String krb5Path,String keytabPath) {
        System.out.println("start DataCleanKerberosImpl");
        // 设置jvm启动时krb5的读取路径参数
        //System.setProperty("java.security.krb5.conf"," /etc/krb5.conf");
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


    public void analyseKerberos(String spStr, int fdSum, String whereStr, String groupStr, String srcDirName,
                                String dstDirName, String hostIp, String hostPort, String hostName,
                                String hostPassword, String user,String krb5Path,
                                String keytabPath, String principalPath) throws TException {

        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;




        try {
            authKrb5(user,krb5Path,keytabPath);
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
                System.out.println();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    public void apriori2Kerberos(String spStr, int fdSum, String pNum, String oNum, String whereStr,
                                 String srcDirName, String dstDirName, String hostIp, String hostPort,
                                 String hostName, String hostPassword, String user, String krb5Path,
                                 String keytabPath, String principalPath) throws TException {



        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String midTable = srcTable + "_mid";
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;

        try {
            authKrb5(user,krb5Path,keytabPath);
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
                HiveUtil.deleteTab(stmt, midTable);
                HiveUtil.deleteTab(stmt, dstTable);
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    public void apriori3Kerberos(String spStr, int fdSum, String pNum, String oNum, String whereStr,
                                 String srcDirName, String dstDirName, String hostIp, String hostPort,
                                 String hostName, String hostPassword, String user, String krb5Path,
                                 String keytabPath,String principalPath) throws TException {



        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String midTable = srcTable + "_mid";
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;

        try {
            authKrb5(user,krb5Path,keytabPath);
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


    public void selectRecKerberos(String spStr, int fdSum, String whereStr, String srcDirName, String dstDirName,
                                  String hostIp, String hostPort, String hostName, String hostPassword,
                                  String user, String krb5Path,String keytabPath, String principalPath) throws TException {


        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;

        try {
            authKrb5(user,krb5Path,keytabPath);
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
            HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);

            HiveUtil.selectRec(stmt, dstDirName, srcTable, dstTable, spStr,fdSum, whereStr);
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


    public void formatFieldKerberos(String spStr, int fdSum, String fdNum, String regExStr, String srcDirName,
                                    String dstDirName, String hostIp, String hostPort, String hostName,
                                    String hostPassword, String user,String krb5Path,
                                    String keytabPath, String principalPath) throws TException {

        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;


        try {
            authKrb5(user,krb5Path,keytabPath);
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


    public void formatRecKerberos(String spStr, int fdSum, String srcDirName, String dstDirName,
                                  String hostIp, String hostPort, String hostName, String hostPassword,
                                  String user, String krb5Path,
                                  String keytabPath, String principalPath) throws TException {

        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;


        try {
            authKrb5(user,krb5Path,keytabPath);
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


    public void selectFieldKerberos(String spStr, int fdSum, String fdNum, String srcDirName,
                                    String dstDirName, String hostIp, String hostPort, String hostName,
                                    String hostPassword, String user, String krb5Path,
                                    String keytabPath,String principalPath) throws TException {



        String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
        String dstTable = srcTable + "_dst";
        String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default"+ ";"+ principalPath;

        Statement stmt = null;
        Connection conn = null;

        try {
            authKrb5(user,krb5Path,keytabPath);
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, hostName,hostPassword);
            stmt = conn.createStatement();
            stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);

            HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);
            HiveUtil.selectField(stmt, dstDirName, srcTable, dstTable, spStr,fdSum, fdNum);
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


    public static void main(String[] args) {

        String hostIp = "192.168.1.166";
        String hostName = "root";
        String hostPassword = "123456";
        int fdSum = 8;
        //数据分隔符
        String spStr = ",";
        String srcDirName = "/test/in/";
        String dstDirName = "/test/out/";
        String hostPort = "10000";

        String user = "hive/cdh166@HADOOP.COM";
        String keytabPath = "/root/hive.keytab";
        String krb5Path = "/etc/krb5.conf";
        String principalPath = "principal=hive/cdh166@HADOOP.COM";

        String fdNum = "8";
        String regExStr = "2";
        System.out.print("---------------------");
        //对不符合规则的数据进行清洗，得到符合字段数目的数据
        DataCleanKerberosImpl dataCleanKerberos = new DataCleanKerberosImpl();
        try {
            dataCleanKerberos.formatFieldKerberos(spStr,fdSum,fdNum,regExStr, srcDirName,dstDirName, hostIp, hostPort, hostName, hostPassword ,user,krb5Path,keytabPath,principalPath);
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.print("结束");


    }


}
