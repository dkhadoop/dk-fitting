package com.dksou.fitting.utils;

import java.io.*;
import java.util.List;

/**
 * Created by Administrator on 2016/3/17 0017.
 */
public class FileUtils {
    /**
     *
     * @param dir 需要遍历的目录
     * @param filter 过滤满足条件的文件
     * @param fileList 存放符合条件的容器
     */
    public static void getFileList(File dir, FilenameFilter filter, List<File> fileList){
        if(dir.exists()){
            File[] files = dir.listFiles();//找到目录下面的所有文件
            for(File file:files){
                //递归
                if(file.isDirectory()){
                    getFileList(file,filter,fileList);
                }else{
                    //对遍历的文件进行过滤，符合条件的放入List集合中
                    if(filter.accept(dir, file.getName())){
                        fileList.add(file);
                    }
                }
            }

        }
    }

    /**
     * 将容器中的文件遍历，写入到目的文件中
     * @param fileList  存放满足条件的文件的集合
     * @param desFile 要写入的目的文件
     */
    public static void write2HdfsSequenceFile(List<File>fileList,File desFile){
        BufferedWriter bw = null;
        try {
            //使用字符流写入到目的文件中
            bw = new BufferedWriter(new FileWriter(desFile));
            //遍历List集合
            for(File file:fileList){
                bw.write(file.getAbsolutePath());//写入目的文件绝对路径
                bw.newLine();
                bw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                if(bw != null){
                    bw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
