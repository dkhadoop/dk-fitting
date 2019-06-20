package com.dksou.fitting.graphx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;

public class LibUtils {
    private static final Logger logger = LoggerFactory.getLogger(LibUtils.class);
    /*
     * 函数名：getFile
     * 作用：使用递归，输出指定文件夹内的所有文件
     * 参数：path：文件夹路径   deep：表示文件的层次深度，控制前置空格的个数
     * 前置空格缩进，显示文件层次结构  List<String>
     */
    public static String[]  getLibJars(String path){
        System.out.println("Libpath = " + path);
        // 获得指定文件对象
        File file = new File(path);
        // 获得该文件夹内的所有文件
        File[] array = file.listFiles();
        ArrayList<String> jars = new ArrayList<>();
        for(int i=0;i<array.length;i++)
        {
            if(array[i].isFile())//如果是文件
            {
                jars.add(array[i].getPath());
                logger.info("Add Jar to classpath : {}" , array[i].getPath());
            }
        }
        String[] jarsarray = jars.toArray(new String[jars.size()]);
        return jarsarray;
    }

    public static void main(String[] args) {
        String path = "D:\\code\\workspace\\dkgit\\freerch-ml\\src\\main\\java\\com\\dksou\\freerch\\ml\\service\\serviceImpl\\als";
        getLibJars(path);
    }
}
