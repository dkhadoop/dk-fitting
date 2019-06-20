package com.dksou.fitting.ml.utils;

import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 *
 * <p>
 * Title: java调用Linux下的shell命令工具
 * </p>
 */
public class RuntimeUtils {
    // Log
//    static Logger log = Logger.getLogger(RuntimeUtils.class);
    /**
     * @param shellCommand
     * @return
     * @throws IOException
     */
    public static void executeShell(String shellCommand,String funcName) {
        BufferedReader bufferedReader = null;
        BufferedReader errorReader = null;
        Process process = null;
        try {
//            log.trace(funcName + " execute running " + shellCommand);
            System.out.println(funcName + " execute running " );
            String[] cmd = { "/bin/sh", "-c", shellCommand };
            // 执行Shell命令
            process = Runtime.getRuntime().exec(cmd);
            if (process != null) {
                bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()), 1024);
                errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()), 1024);
                process.waitFor();
            }
            String line = null;
            // 读取Shell的输出内容，并添加到stringBuffer中
            while (bufferedReader != null && (line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }
            while (errorReader != null && (line = errorReader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (Exception e) {
//            log.error(ioe.getMessage());
            e.printStackTrace();
//            log.error(e,e.fillInStackTrace());
        } finally {
            System.out.println(funcName + " End !");

            process.destroy();
            if (bufferedReader != null) {
                IOUtils.closeQuietly(bufferedReader);
            }
            if (errorReader != null) {
                IOUtils.closeQuietly(errorReader);
            }
        }


    }

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // String fileName = args[0];
        // 调用shell命令上传文件
//        String cmd = "aftstcp 1 ABISFILE %s 01@000";
        // String result = RuntimeUtils.executeShell(String.format(cmd, fileName));
//        System.out.println("args = " + RuntimeUtils.executeShell("ls"));
    }
}