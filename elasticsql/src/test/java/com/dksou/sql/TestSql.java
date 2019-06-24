package com.dksou.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by myy on 2016/10/28.
 */
public class TestSql {
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "mysql");
        Connection connection = DriverManager.getConnection("jdbc:calcite:model=" +
                "E:\\fitting13\\dk-essql\\src\\test\\java\\com\\dksou\\sql\\elasticsearch-zips-model3.json", info);
        ResultSet result = connection.getMetaData().getTables(null, null, null, null);
        while (result.next()) {
            System.out.println("Database : " + result.getString(2) + ",Table : " + result.getString(3));
        }

        Statement statement = connection.createStatement();
        // "SELECT avg(prod_price),max(prod_price),min(prod_price),sum(prod_price),count(prod_price) FROM Products limit 20";
//        String sql = "SELECT * FROM Products limit 400";  //SELECT vend_name,prod_name,prod_price FROM vendors INNER JOIN products ON vendors.vend_id = products.vend_id limit 20
//        String sql = "SELECT count(prod_price),avg(prod_price),sum(prod_price),max(prod_price),min(prod_price) from Products ";
        //SELECT vend_name,prod_name,prod_price FROM vendors INNER JOIN products ON vendors.vend_id = products.vend_id limit 20
        String sql = "select * from account";
        ResultSet resultSet = statement.executeQuery(sql); //WHERE  order_num >= 2000 AND cust_id<=20009 ORDER BY cust_id ,count(order_num)
//                "select * from ss.zips limit 21");


//                "select city, case when city='FLORENCE' then '111' else '222' end c from ZIPS ");
        //OFFSET 1 FETCH NEXT 30 ROW ONLY
        int count = 0;
        System.out.println("================================================");
        System.out.println(sql);
        StringBuffer str2 = new StringBuffer();
        int bt = 1;
        while (resultSet.next()) {
            count++;
//            System.out.println(" ============================================================== " + count);
//            System.out.println("resultSet = " + resultSet.getString(1));

            int columnCount = resultSet.getMetaData().getColumnCount();
            StringBuffer str = new StringBuffer();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = resultSet.getMetaData().getColumnName(i);
                Object value = resultSet.getObject(i);
                System.out.print("columnName = " + columnName + "  ----->  ");
                System.out.println("value = " + value);
            }

//            System.out.println(str.toString());
//            System.out.println( 6.823333333333333);
        }
        resultSet.close();
        statement.close();
        connection.close();
    }
}
