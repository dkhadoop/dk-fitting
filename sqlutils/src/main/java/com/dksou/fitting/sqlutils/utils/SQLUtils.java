package com.dksou.fitting.sqlutils.utils;

import com.dksou.fitting.sqlutils.service.DKSQLConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2016/3/18 0018.
 */
public class SQLUtils {
    private static final String JDBC_DRIVER_HIVE = "org.apache.hive.jdbc.HiveDriver";
    private static final String JDBC_DRIVER_IPMAPA = "org.apache.hive.jdbc.HiveDriver";
    private static String krb5_conf;
    private static String principal;
    private static String keytab;

    //static String CONNECTION_URL_IPMAPA = "jdbc:impala://192.168.1.126:21050/;auth=noSasl";
    //private static String url = "jdbc:hive2://192.168.1.166:10000/default;principal=hive/cdh166@EXAMPLE.COM";// hive库地址+库名
    public static Connection connectionHive(String hostIP, String port, String username, String password, String database, String queueName, DKSQLConf dkSqlConf) throws Exception {
        krb5_conf = dkSqlConf.KRB5_CONF;
        principal = dkSqlConf.PRINCIPAL;
        keytab = dkSqlConf.KEYTAB;
        String url = "jdbc:hive2://" + hostIP + ":" + port + "/" + database;
        //kerberos认证
        if (krb5_conf != null && !"".equals(krb5_conf) && principal != null && !"".equals(principal) && keytab != null && !"".equals(keytab)) {
            authKrb5();
            url += ";principal=" + principal;
        }
        Class.forName(JDBC_DRIVER_HIVE);
        Properties conn_info = new Properties();
        conn_info.setProperty("user", username);
        conn_info.setProperty("password", password);
        if (queueName != null && !"".equals(queueName)) {
            conn_info.setProperty("hiveconf:mapreduce.job.queuename", queueName);
        }
        return DriverManager.getConnection(url, conn_info);
    }

    //jdbc:impala://192.168.0.22:21050/db_1
    //static String CONNECTION_URL_IPMAPA = "jdbc:impala://192.168.1.126:21050/;auth=noSasl";
    public static Connection connectionImpala(String hostIP, String port, String database, DKSQLConf dkSqlConf) throws Exception {
        krb5_conf = dkSqlConf.KRB5_CONF;
        principal = dkSqlConf.PRINCIPAL;
        keytab = dkSqlConf.KEYTAB;
        String url = "jdbc:hive2://" + hostIP + ":" + port + "/" + database + ";auth=noSasl";
        //kerberos认证
        if (krb5_conf != null && !"".equals(krb5_conf) && principal != null && !"".equals(principal) && keytab != null && !"".equals(keytab)) {
            authKrb5();
            url += ";principal=" + principal;
        }
        Class.forName(JDBC_DRIVER_IPMAPA);
        return DriverManager.getConnection(url);

    }


    public static List<String> getColumns(int i, ResultSet rs) throws Exception {
        ResultSetMetaData metaData = rs.getMetaData();
        List<String> clist = new ArrayList<>();
        while (true) {
            String col = "";
            try {
                col = metaData.getColumnName(i);
                clist.add(col);
                i++;
            } catch (Exception e) {
                break;
            }
        }
        return clist;
    }

    private static void authKrb5() {
        String result = "Succeeded in authenticating through Kerberos!";
        // 设置jvm启动时krb5的读取路径参数
        // System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        System.setProperty("java.security.krb5.conf", krb5_conf);
        // 配置kerberos认证
        Configuration conf = new Configuration();
        conf.setBoolean("hadoop.security.authorization", true);
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
            result = "Exception in authenticating through Kerberos!";
            e.printStackTrace();
        }
        System.out.println(result);
    }
}
