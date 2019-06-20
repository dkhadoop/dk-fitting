package com.dksou.fitting.datasource.util;

import com.dksou.fitting.datasource.service.ResultEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by Administrator on 2016/1/19 0019.
 */
public class SSHImportUtils {
    private static Logger logger = org.apache.log4j.Logger.getLogger(SSHImportUtils.class);
    private static String sqoopBinHome = "";

    static {
        Properties hadoopProp = PropUtil.loadProp(System.getProperty("user.dir") + "/conf/datasource.properties");
        sqoopBinHome = hadoopProp.getProperty("datasource.sqoop_bin_home");
    }

    //sqoop import --driver com.microsoft.sqlserver.jdbc.SQLServerDriver --connect "jdbc:sqlserver://192.168.1.21:1433;database=customer;username=sa;password=Zgc172531" --table customer --target-dir /home/sqooptest/sqlserver
    //-D mapred.job.queue.name=lesson
    public static ResultEntity rdbmsToHdfs(String connect, String username, String password, String table, String targetDir, int writeMode, String checkColumn, String lastValue,
                                           String queueName, String numMappers, String splitBy, String where, String fileSeparator, String extra, String principal, String keytab) {
        ResultEntity rs = new ResultEntity();
        if (connect.contains("oracle"))
            table = table.toUpperCase();

        if (StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(keytab)) {
            authKrb5(principal, keytab);
        }
        String sqoopHome = sqoopBinHome.replaceAll("\\\\", "/");
        if (sqoopHome.length() > 0 && !sqoopHome.endsWith("/")) {
            sqoopHome = sqoopHome + "/";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqoopHome + "sqoop import");
        if (StringUtils.isNotBlank(queueName)) {
            sb.append(" -D mapred.job.queue.name=").append(queueName).append(" ");
        }
        sb.append(" --connect '" + connect + "'");
        sb.append(" --username ").append(username);
        sb.append(" --password ").append(password);
        if (StringUtils.isNotBlank(table)) {
            sb.append(" --table ").append(table);
        }
        if (StringUtils.isNotBlank(where)) {
            sb.append(" --where \"").append(where).append("\" ");
        }
        if (StringUtils.isNotBlank(splitBy)) {
            sb.append(" --split-by \"").append(splitBy).append("\" ");
        }
        if (StringUtils.isNotBlank(extra)) {
            sb.append(" " + extra + " ");
        }
        sb.append(" --target-dir ").append(targetDir);
        sb.append(" --m ").append(numMappers);
        sb.append(" --fields-terminated-by '").append(fileSeparator + "'");
        if (writeMode == 0) {
            sb.append(" --delete-target-dir ");
        } else if (writeMode == 1) {
            sb.append(" --check-column ").append(checkColumn);
            sb.append(" --last-value ").append(lastValue);
            sb.append(" --incremental ").append("append");
        } else if (writeMode == 2) {
            sb.append(" --check-column ").append(checkColumn);
            sb.append(" --last-value '").append(lastValue + "'");
            sb.append(" --incremental ").append("lastmodified");
            sb.append(" --append ");
        } else {
            logger.error("writeMode parameter error, please check!");
            rs.setMessage("writeMode parameter error, please check!");
            return rs;
        }
        sb.append(" --null-string \"\" ");
        System.out.println("exe = " + sb.toString());
        rs = SshUtil.exe(rs, sb.toString());
        return rs;
    }

    public static ResultEntity rdbmsToHive(String connect, String username, String password, String table, String hTable, int writeMode, String checkColumn, String lastValue,
                                           String queueName, String numMappers, String splitBy, String where, String fileSeparator, String extra, String principal, String keytab) {
        ResultEntity rs = new ResultEntity();
        if (connect.contains("oracle"))
            table = table.toUpperCase();
        //kerberos安全认证
        if (StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(keytab)) {
            authKrb5(principal, keytab);
        }
        String sqoopHome = sqoopBinHome.replaceAll("\\\\", "/");
        if (sqoopHome.length() > 0 && !sqoopHome.endsWith("/")) {
            sqoopHome = sqoopHome + "/";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqoopHome + "sqoop import");
        if (StringUtils.isNotBlank(queueName)) {
            sb.append(" -D mapred.job.queue.name=").append(queueName).append(" ");
        }
        sb.append(" --connect '" + connect + "'");
        sb.append(" --username ").append(username);
        sb.append(" --password ").append(password);
        if (StringUtils.isNotBlank(table)) {
            sb.append(" --table ").append(table);
        }
        if (StringUtils.isNotBlank(where)) {
            sb.append(" --where \"").append(where).append("\" ");
        }
        if (StringUtils.isNotBlank(splitBy)) {
            sb.append(" --split-by \"").append(splitBy).append("\" ");
        }
        if (extra != null && !"".equals(extra)) {
            sb.append(" " + extra + " ");
        }
        sb.append(" --hive-table ").append(hTable);
        sb.append(" --m ").append(numMappers);
        sb.append(" --fields-terminated-by '").append(fileSeparator + "'");
        sb.append(" --hive-import ");
        if (writeMode == 0) {
            sb.append(" --hive-overwrite ");
        } else if (writeMode == 1) {
            rs.setMessage("Hive增量导入不支持Append增量导入模式，如果是内部表可直接导入HDFS!");
            return rs;
        } else if (writeMode == 2) {
            sb.append(" --check-column ").append(checkColumn);
            sb.append(" --last-value '").append(lastValue + "'");
            sb.append(" --incremental ").append("lastmodified");
        } else {
            logger.error("writeMode parameter error, please check!");
            rs.setMessage("writeMode parameter error, please check!");
            return rs;
        }
        sb.append(" --null-string \"\" ");
        System.out.println("exe = " + sb.toString());
        rs = SshUtil.exe(rs, sb.toString());
        return rs;
    }

    public static ResultEntity rdbmsToHbase(String connect, String username, String password, String table, String hTable, String col, String family, int writeMode,
                                            String checkColumn, String lastValue, String queueName, String numMappers, String splitBy, String where, String extra, String principal, String keytab) {
        ResultEntity rs = new ResultEntity();
        if (connect.contains("oracle"))
            table = table.toUpperCase();
        String authKrb5_res = "";
        //kerberos安全认证
        if (StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(keytab)) {
            authKrb5(principal, keytab);
        }
        String sqoopHome = sqoopBinHome.replaceAll("\\\\", "/");
        if (sqoopHome.length() > 0 && !sqoopHome.endsWith("/")) {
            sqoopHome = sqoopHome + "/";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqoopHome + "sqoop import");
        if (StringUtils.isNotBlank(queueName)) {
            sb.append(" -D mapred.job.queue.name=").append(queueName).append(" ");
        }
        sb.append(" --connect '" + connect + "'");
        sb.append(" --username ").append(username);
        sb.append(" --password ").append(password);
        if (StringUtils.isNotBlank(table)) {
            sb.append(" --table ").append(table);
        }
        if (StringUtils.isNotBlank(where)) {
            sb.append(" --where \"").append(where).append("\" ");
        }
        if (StringUtils.isNotBlank(splitBy)) {
            sb.append(" --split-by \"").append(splitBy).append("\" ");
        }
        if (extra != null && !"".equals(extra)) {
            sb.append(" " + extra + " ");
        }
//      sb.append(" --hbase-create-table ");
        sb.append(" --hbase-row-key ").append(col);
        sb.append(" --hbase-table ").append(hTable);
        sb.append(" --column-family ").append(family);
        sb.append(" --m ").append(numMappers);
        if (writeMode == 0) {
            // sb.append(" --delete-target-dir ");
        } else if (writeMode == 1) {
            sb.append(" --check-column ").append(checkColumn);
            sb.append(" --last-value ").append(lastValue);
            sb.append(" --incremental ").append("append");
        } else if (writeMode == 2) {
            sb.append(" --check-column ").append(checkColumn);
            sb.append(" --last-value '").append(lastValue + "'");
            sb.append(" --incremental ").append("lastmodified");
            //sb.append(" --append ");
        } else {
            rs.setMessage("writeMode parameter error, please check!");
            return rs;
        }
        sb.append(" --hbase-bulkload ");
        sb.append(" --null-string \"\" ");
        System.out.println("exe = " + sb.toString());
        rs = SshUtil.exe(rs, sb.toString());
        return rs;
    }


    public static ResultEntity hdfsToRdbms(String connect, String username, String password, String table, String exportDir, String queueName, int writeMode, String updateKey,
                                           String numMappers, String fileSeparator, String extra, String principal, String keytab) {
        ResultEntity rs = new ResultEntity();
        if (connect.contains("oracle"))
            table = table.toUpperCase();
        //kerberos安全认证
        if (StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(keytab)) {
            authKrb5(principal, keytab);
        }
        String sqoopHome = sqoopBinHome.replaceAll("\\\\", "/");
        if (sqoopHome.length() > 0 && !sqoopHome.endsWith("/")) {
            sqoopHome = sqoopHome + "/";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqoopHome + "sqoop export");
        if (StringUtils.isNotBlank(queueName)) {
            sb.append(" -D mapred.job.queue.name=").append(queueName).append(" ");
        }
        sb.append(" --connect '" + connect + "'");
        sb.append(" --username ").append(username);
        sb.append(" --password ").append(password);
        if (StringUtils.isNotBlank(table)) {
            sb.append(" --table ").append(table);
        }
        if (writeMode == 0) {

        } else if (writeMode == 1) {
            if (StringUtils.isNotBlank(updateKey)) {
                sb.append(" --update-key \"").append(updateKey).append("\" ");
            } else {
                rs.setMessage("如果设置writeMode为1，则updateKey参数必填");
                return rs;
            }
        } else if (writeMode == 2) {
            if (StringUtils.isNotBlank(updateKey)) {
                sb.append(" --update-key \"").append(updateKey).append("\" ");
                sb.append(" --update-mode allowinsert");
            } else {
                rs.setMessage("如果设置writeMode为2，则updateKey参数必填");
                return rs;
            }
        } else {
            rs.setMessage("writeMode parameter error, please check!");
            return rs;
        }
//      sb.append(" --direct ");
        sb.append(" --batch ");
        sb.append(" --export-dir ").append(exportDir);
        sb.append(" --m ").append(numMappers);
        sb.append(" --fields-terminated-by '").append(fileSeparator + "'");
        if (StringUtils.isNotBlank(extra)) {
            sb.append(" " + extra + " ");
        }
        System.out.println("exe = " + sb.toString());
        rs = SshUtil.exe(rs, sb.toString());
        return rs;
    }

    private static void authKrb5(String principal, String keytabPath) {
        StringBuilder sb = new StringBuilder();
        sb.append("kinit -k -t").append(" ").append(keytabPath).append(" ").append(principal);
        try {
            SshUtil.exe(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void rdbmsToEs(String connect, String username, String password, String table, String esIpAndPort, String
            esClusterName, String esIndexName, String esTypeName, String numMappers, String where, String queueName) {
        if (connect.contains("oracle"))
            table = table.toUpperCase();
        String dirName = esIpAndPort + "@" + esClusterName + "@" + esIndexName + "/" + esTypeName;

        String sqoopHome = sqoopBinHome.replaceAll("\\\\", "/");
        if (sqoopHome.length() > 0 && !sqoopHome.endsWith("/")) {
            sqoopHome = sqoopHome + "/";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqoopHome + "sqoop import");
        if (queueName != null && !"".equals(queueName)) {
            sb.append(" -D mapred.job.queue.name=").append(queueName).append(" ");
        }
        sb.append(" --connect '" + connect + "'");
        sb.append(" --username ").append(username);
        sb.append(" --password ").append(password);
        if (StringUtils.isNotBlank(table)) {
            sb.append(" --table ").append(table);
        }
        if (where != null && !"".equals(where)) {
            sb.append(" --where \"").append(where).append("\" ");
        }

        sb.append(" -target-dir ").append(dirName);
        sb.append(" -m ").append(numMappers);
        sb.append(" --null-string \"\" ");
//        sb.append(" --default-character-set=").append(charset+"");
        System.out.println("exe = " + sb.toString());
//        System.out.println("sb.toString() = " + sb.toString());
//        String exe = JavaShellUtil.executeShell(sb.toString());
        SshUtil.exe(sb.toString());
    }


    //jdbc:sqlserver://192.168.4.155;username=sa;password=sa;database=pi --table=pi_item_master --target-dir /user/cloudcomputing/output -m 3
    public static void sqlServerToEs(String connect, String table, String esIpAndPort,
                                     String esClusterName, String esIndexName, String esTypeName, String numMappers, String where,
                                     String queueName) {

        if (connect.contains("oracle"))
            table = table.toUpperCase();
        String dirName = esIpAndPort + "@" + esClusterName + "@" + esIndexName + "/" + esTypeName;
        String sqoopHome = sqoopBinHome.replaceAll("\\\\", "/");
        if (sqoopHome.length() > 0 && !sqoopHome.endsWith("/")) {
            sqoopHome = sqoopHome + "/";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqoopHome + "sqoop import");
        if (queueName != null && !"".equals(queueName)) {
            sb.append(" -D mapred.job.queue.name=").append(queueName).append(" ");
        }
        sb.append(" --connect '" + connect + "'");
        if (StringUtils.isNotBlank(table)) {
            sb.append(" --table ").append(table);
        }
        if (where != null && !"".equals(where)) {
            sb.append(" --where \"").append(where).append("\" ");
        }
        sb.append(" -target-dir ").append(dirName);
        sb.append(" -m ").append(numMappers);
        sb.append(" --null-string \"\" ");
        System.out.println("exe = " + sb.toString());
        SshUtil.exe(sb.toString());
    }


    public static void hdfsToEs(String esIpAndPort, String esClusterName, String esIndexName, String esTypeName,
                                String exportDir, String numMappers, String fileSeparator, String queueName) {
        String jdbcStr = "es@" + esIpAndPort + "@" + esClusterName + "@" + esIndexName + "/" + esTypeName;
        String sqoopHome = sqoopBinHome.replaceAll("\\\\", "/");
        if (sqoopHome.length() > 0 && !sqoopHome.endsWith("/")) {
            sqoopHome = sqoopHome + "/";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqoopHome + "sqoop export");
        if (queueName != null && !"".equals(queueName)) {
            sb.append(" -D mapred.job.queue.name=").append(queueName).append(" ");
        }
        sb.append(" --connect '" + jdbcStr + "'");
        sb.append(" --export-dir ").append(exportDir);
        sb.append(" -m ").append(numMappers);
        sb.append(" --fields-terminated-by '").append(fileSeparator + "'");
        System.out.println("exe = " + sb.toString());
        SshUtil.exe(sb.toString());
    }


}