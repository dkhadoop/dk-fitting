package com.dksou.essql.utils;

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
        try(Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
//            ResultSet result2 = connection.getMetaData().getTables( null, null, null, null);
//            while( result2.next()) {
//                System. out.println( "Database : " + result2.getString(2) + ",Table : " + result2.getString(3));
//            }
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            return function.apply(resultSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
