package com.dksou.fitting.nlp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.util.Properties;


@SpringBootApplication
public class Application {

    static {
        LoadNLPData();
    }

    // 程序启动入口
    // 启动嵌入式的 Tomcat 并初始化 Spring 环境及其各 Spring 组件
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);

    }

    public static void LoadNLPData(){
        String projectPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();//读取项目路径
        String hanlpPriperties = projectPath+"nlp.properties";//hanlp.properties文件路径
        String hanlpData = projectPath+"NLP/";//hanlp字典路径
//        System.out.println("projectPath---"+projectPath);
//        System.out.println("hanlpPriperties---"+hanlpPriperties);
//        System.out.println("hanlpData---"+hanlpData);

        try {
            WriteProperties(hanlpPriperties,"root",hanlpData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String root1 = GetValueByKey(hanlpPriperties, "root");
//        System.out.println("root1---"+root1);
//        System.out.println(HanLP.segment("你好，欢迎使用HanLP汉语处理包！"));
    }


    //根据Key读取Value
    public static String GetValueByKey(String filePath, String key) {
        Properties pps = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(filePath));
            pps.load(in);
            String value = pps.getProperty(key);
            System.out.println(key + " = " + value);
            return value;

        }catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    //写入Properties信息
    public static void WriteProperties (String filePath, String pKey, String pValue) throws IOException {
        Properties pps = new Properties();

        InputStream in = new FileInputStream(filePath);
        //从输入流中读取属性列表（键和元素对）
        pps.load(in);
        //调用 Hashtable 的方法 put。使用 getProperty 方法提供并行性。
        //强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。
        OutputStream out = new FileOutputStream(filePath);
        pps.setProperty(pKey, pValue);
        //以适合使用 load 方法加载到 Properties 表中的格式，
        //将此 Properties 表中的属性列表（键和元素对）写入输出流
        pps.store(out, "Update " + pKey + " name");

    }


}
