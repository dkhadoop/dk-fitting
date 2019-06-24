package com.dksou.sql;

import com.dksou.essql.utils.CalciteUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SQLTest {
    private String[] sqlArray = {
            "select max(age) as maxAge, min(age) as minAge, count(\"name.raw\") as cnt from person",
            "select * from person order by age desc offset 2 rows fetch next 10 rows only",
            "select order_num,item_price  from orderitems where order_num >=20008 order by item_price desc",
            "SELECT prod_id,prod_price,prod_name FROM products " +
                    "WHERE   prod_price <= 4 OR prod_name='King doll'",
            "SELECT prod_name,prod_price,vend_id FROM products WHERE vend_id = 'DLL01'",
            "SELECT vend_name,vend_city,vend_zip FROM vendors" +
                    "WHERE vend_zip IN ('44444','44333') ORDER BY vend_city",
            "SELECT vend_name,vend_city,vend_zip FROM vendors" +
                    "WHERE vend_zip NOT IN ('44444','44333') ORDER BY vend_city", //不支持
            "SELECT prod_id,prod_name\n" +
                    "FROM products WHERE prod_name LIKE 'Fish%'",
            "SELECT prod_id,prod_name FROM products WHERE prod_name LIKE '__ inch teddy bear'",
            "SELECT UPPER(vend_name) as lowwer_name from vendors",
            "SELECT CONCAT(vend_name,vend_city) FROM vendors ORDER BY vend_name",   //不支持
            "SELECT count(vend_city) FROM vendors",
            "SELECT prod_id,quantity,item_price,quantity*item_price AS expanded_price FROM OrderItems WHERE order_num = 20008", //不支持
            "select DATE_FORMAT(order_date,'%Y') FROM orders",          //不支持
            "SELECT vend_name,LEFT(vend_name,2) AS Left_2 FROM Vendors ORDER BY vend_name",          //不支持
            "SELECT  item_price,EXP(item_price) ,SQRT(item_price),ABS(item_price) FROM orderitems",
            "SELECT  count(*)  FROM products",  //不支持
            "SELECT  DISTINCT prod_price FROM Products",
            "SELECT prod_price from products where prod_price >3",
            "select order_num,item_price  from orderitems where order_num >=20008 and item_price > 3.49 order by item_price desc",
            "SELECT prod_price,count(prod_price) FROM products group by prod_price order by prod_price desc",
            "select prod_price from products order by prod_price desc ",
            "SELECT count(order_num) as ss ,max(order_num),min(order_num),avg(order_num) as agn FROM orders"
    };

    private Properties properties = new Properties();



    @Before
    public void prepare() {
        String jsonPath = "D:\\code\\workspace\\dkgit\\dk-essql\\src\\main\\resources\\elasticsearch-zips-model3.json";
        if (jsonPath.startsWith("file:"))
            jsonPath = jsonPath.substring("file:".length());

        properties.put("model", jsonPath);
        //in order to use '!=', otherwise we can just use '<>'
        properties.put("conformance", "ORACLE_10");
        //set this but useless. wonder why
        properties.put("caseSensitive", "false");
    }

    @Test
    public void testSelect() {
        long startTime = System.currentTimeMillis();
        String sql = sqlArray[sqlArray.length - 4];
        System.out.println("sql: " + sql);
        CalciteUtil.execute(properties, sql, (Function<ResultSet, Void>) resultSet -> {
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int count = metaData.getColumnCount();
                while (resultSet.next()) {
                    StringBuffer str = new StringBuffer();
                    for (int i = 1; i <= count; ++i) {
//                        str.append(resultSet.getObject(i).toString());
                        System.out.print(metaData.getColumnLabel(i).toLowerCase() + ": " +
                                (resultSet.getObject(i) != null ? resultSet.getObject(i).toString() : "null") + "    ");
                    }
                    System.out.println(str.toString());
                }
                long endTime = System.currentTimeMillis();
                System.out.println("Time: " + (endTime - startTime) + " ms");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

//    @Test
    public void testConcurrentSelect() {
        ExecutorService executorService = Executors.newFixedThreadPool(sqlArray.length);
        for (String sql : sqlArray)
            executorService.execute(new QueryRunnable(properties, sql));
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class QueryRunnable implements Runnable {
        private Properties properties;
        private String sql;

        public QueryRunnable(Properties properties, String sql) {
            this.properties = properties;
            this.sql = sql;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            CalciteUtil.execute(properties, sql, (Function<ResultSet, Void>) resultSet -> {
                try {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int count = metaData.getColumnCount();
                    synchronized (SQLTest.class) {
                        System.out.println(Thread.currentThread().getName() + ", sql: " + sql);
                        while (resultSet.next()) {
                            for (int i = 1; i <= count; ++i)
                                System.out.print(metaData.getColumnLabel(i).toLowerCase() + ": " +
                                        (resultSet.getObject(i) != null ? resultSet.getObject(i).toString() : "null") + "    ");
                            System.out.println();
                        }
                        long endTime = System.currentTimeMillis();
                        System.out.println("Time: " + (endTime - startTime) + " ms");
                        System.out.println();
                        System.out.println();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            });
        }
    }

}
