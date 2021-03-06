package com.dksou.fitting.datasource.util;

import java.io.*;
import java.util.Properties;

public class PropUtil {

    public static Properties loadProp(String propFilePath) {
        Properties p = new Properties();
        try {
            // InputStream inputStream = PropUtil.class.getClassLoader().getResourceAsStream(propFilePath);
            InputStream inputStream = new FileInputStream(new File(propFilePath));
            p.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }

    public static void main(String[] args) {
        Properties properties = PropUtil.loadProp("D:\\workspace\\fitting\\fitting-datasource\\src\\main\\resources\\datasource.properties");
        String property = properties.getProperty("datasource.port");
        System.out.println(property);
    }
}
