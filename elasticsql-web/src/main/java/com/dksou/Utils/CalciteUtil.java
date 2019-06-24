package com.dksou.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.function.Function;

public class CalciteUtil
{
    public static <T> T execute(Properties properties, String sql, Function<ResultSet, T> function)
    {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try(Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
//            ResultSet resul 2 = connection.getMetaData().getTables( null, null, null, null);
//            while( result2.next()) {
//                System. out.println( "Database : " + result2.getString(2) + ",Table : " + result2.getString(3));
//            }
//            connection.getMetaData().getTables()
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            return function.apply(resultSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
