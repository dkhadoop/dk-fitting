package com.dksou.fitting.sqlutils.service.serviceimpl;

import com.dksou.fitting.sqlutils.service.DKSQLEngine;
import com.dksou.fitting.sqlutils.service.DKSQLConf;
import com.dksou.fitting.sqlutils.service.ResultEntity;
import com.dksou.fitting.sqlutils.utils.SQLUtils;
import net.sf.json.JSONArray;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DKSQLEngineImpl implements DKSQLEngine.Iface {
    private static Logger logger = Logger.getLogger(DKSQLEngineImpl.class);


    /**
     * @param hostIp    Hive连接ip地址
     * @param port      Hive连接端口
     * @param username  用户名
     * @param password  密码
     * @param database  数据库
     * @param sql       要执行的sql语句
     * @param queueName 指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置）
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return ResultEntity
     */
    @Override
    public ResultEntity executeHQL(String hostIp, String port, String username, String password, String database, String sql, String queueName, DKSQLConf dkSqlConf) {
        ResultEntity rs = new ResultEntity();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SQLUtils.connectionHive(hostIp, port, username, password, database, queueName, dkSqlConf);
            stmt = conn.createStatement();
            System.out.println("执行的SQL:" + sql);
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
        }
        rs.setStatus(0);
        return rs;
    }

    /**
     * @param hostIp    Impala连接ip地址
     * @param port      Impala连接端口
     * @param database  要连接的数据库
     * @param sql       需要执行的sql
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return ResultEntity
     */
    @Override
    public ResultEntity excuteISQL(String hostIp, String port, String database, String sql, DKSQLConf dkSqlConf) {
        ResultEntity rs = new ResultEntity();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SQLUtils.connectionImpala(hostIp, port, database, dkSqlConf);
            stmt = conn.createStatement();
            System.out.println("执行的SQL:" + sql);
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
        }
        rs.setStatus(0);
        return rs;
    }

    /**
     * @param hostIp    hive 的连接IP地址
     * @param port      hive的连接端口
     * @param username  用户名
     * @param password  密码
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param orderBy   排序字段
     * @param startRow  开始行
     * @param endRow    结束行
     * @param queueName 指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置）
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return ResultEntity
     */
    @Override
    public ResultEntity excuteQueryHQL(String hostIp, String port, String username, String password, String database, String sql, String orderBy, int startRow, int endRow, String queueName, DKSQLConf dkSqlConf) {
        ResultEntity rs = new ResultEntity();
        String result = "";
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SQLUtils.connectionHive(hostIp, port, username, password, database, queueName, dkSqlConf);
            stmt = conn.createStatement();
            //select * from  (select row_number() over (order by id) as rnum, tb.* from (select * from customer where id>2) as tb)t where rnum between '1' and '2';
            sql = "select * from  (select row_number() over (order by " + orderBy + ") as rnum, tb.* from (" + sql + ") as tb)t where rnum between \'" + startRow + "\' and \'" + endRow + "\'";
            System.out.println("执行的SQL:" + sql);
            ResultSet resultSet = stmt.executeQuery(sql);
            List<String> columns = SQLUtils.getColumns(2, resultSet);
            List<Map<String, String>> resList = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, String> tempMap = new HashMap<>();
                for (String columnLable : columns) {
                    tempMap.put(columnLable.substring(2, columnLable.length()), resultSet.getString(columnLable));
                }
                resList.add(tempMap);
            }
            result = JSONArray.fromObject(resList).toString();
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
        }
        rs.setStatus(0);
        rs.setResult(result);
        return rs;
    }

    /**
     * @param hostIp    Impala的连接IP地址
     * @param port      Impala连接端口
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return ResultEntity
     */
    @Override
    public ResultEntity excuteQueryISQL(String hostIp, String port, String database, String sql, DKSQLConf dkSqlConf) {
        ResultEntity rs = new ResultEntity();
        String result = "";
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SQLUtils.connectionImpala(hostIp, port, database, dkSqlConf);
            stmt = conn.createStatement();
            System.out.println("执行的SQL:" + sql);
            ResultSet resultSet = stmt.executeQuery(sql);
            List<String> columns = SQLUtils.getColumns(1, resultSet);
            List<Map<String, String>> resList = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, String> tempMap = new HashMap<>();
                for (String columnLable : columns) {
                    tempMap.put(columnLable, resultSet.getString(columnLable));
                }
                resList.add(tempMap);
            }
            result = JSONArray.fromObject(resList).toString();
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
        }
        rs.setResult(result);
        rs.setStatus(0);
        return rs;
    }

    /**
     * @param hostIp    hive的连接IP地址
     * @param port      hive的连接端口
     * @param username  用户名
     * @param password  密码
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param queueName 指定MapReduce作业提交到的队列（为mapreduce.job.queuename此参数设置）
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return ResultEntity
     */
    @Override
    public ResultEntity countHQL(String hostIp, String port, String username, String password, String database, String sql, String queueName, DKSQLConf dkSqlConf) {
        ResultEntity rs = new ResultEntity();
        String result = "";
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = SQLUtils.connectionHive(hostIp, port, username, password, database, queueName, dkSqlConf);
            stmt = conn.createStatement();
            System.out.println("执行的SQL:" + sql);
            ResultSet resultSet = stmt.executeQuery(sql);
            long count = 0L;
            while (resultSet.next()) {
                count = resultSet.getLong(1);
            }
            result = count + "";
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
        }
        rs.setStatus(0);
        rs.setResult(result);
        return rs;
    }

    /**
     * @param hostIp    Impala的连接IP地址
     * @param port      Impala的连接端口
     * @param database  要连接的数据库
     * @param sql       要执行的sql
     * @param dkSqlConf DKSQL配置项，包括：kerberos的krb5.conf文件路径配置，principal主体名配置，keytab文件路径配置
     * @return ResultEntity
     */
    @Override
    public ResultEntity countIQL(String hostIp, String port, String database, String sql, DKSQLConf dkSqlConf) {
        ResultEntity rs = new ResultEntity();
        String result = "";
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SQLUtils.connectionImpala(hostIp, port, database, dkSqlConf);
            stmt = conn.createStatement();
            System.out.println("执行的SQL:" + sql);
            ResultSet resultSet = stmt.executeQuery(sql);
            long count = 0L;
            while (resultSet.next()) {
                count = resultSet.getLong(1);
            }
            result = count + "";
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
        }
        rs.setResult(result);
        rs.setStatus(0);
        return rs;
    }
}
