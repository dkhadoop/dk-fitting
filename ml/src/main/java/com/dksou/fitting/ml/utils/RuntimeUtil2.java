package com.dksou.fitting.ml.utils;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.UUID;

/**
 * Runtime工具类，用于本机执行某些命令<br>
 *
 * 重定向标准输入输出和错误流
 *
 */
public class RuntimeUtil2 {
    // Log
    static Logger log = Logger.getLogger(RuntimeUtil2.class);
    // 当前Runtime
    private static Runtime runtime = Runtime.getRuntime();

    // 工具类不可实例化
    private RuntimeUtil2() {
    }

    /**
     * 以非阻塞模式执行命令
     * @param callback   回调类，当命令全部执行完成后会执行其onExit方法，null表示不进行回调
     * @param envp    运行的上下文环境变量，每项都应该写成name=value的格式；null表示直接继承当前Java进程的全部环境变量
     * @param dir     命令执行的工作目录；null表示继承当前Java进程的工作目录
     * @param startCommand     起始命令
     * @param commands     其他后续命令，如果有设置，会使用管道来关联前后命令的标准输出流和标准输入流
     */
    public static void runAsUnblocking(final RuntimeCallback callback, final String[] envp, final File dir,
                                       final String[] startCommand, final String[]... commands) {
        // 非阻塞的实现就是交给其他函数执行
        new Thread(new Runnable() {
            @Override
            public void run() {
                StringReader result = RuntimeUtil2.run(envp, dir, startCommand, commands);

                // 如果需要回调的话，调用回调函数
                if (callback != null) {
                    callback.onExit(result, envp, dir, startCommand, commands);
                }
            }
        }).start();
    }

    /**
     * 以阻塞模式执行命令
     * @param envp     运行的上下文环境变量，每项都应该写成name=value的格式；null表示直接继承当前Java进程的全部环境变量
     * @param dir     命令执行的工作目录；null表示继承当前Java进程的工作目录
     * @param startCommand     起始命令
     * @param commands     其他后续命令，如果有设置，会使用管道来关联前后命令的标准输出流和标准输入流
     * @return 命令执行后的最终结果
     */
    public static StringReader run(String[] envp, File dir, String[] startCommand, String[]... commands) {
        // 生成一个命令id，只是为日志区别用
        String commandId = UUID.randomUUID().toString();
        log.warn("Command(" + commandId + ") start");
        StringBuffer result = new StringBuffer();
        Process currentProcess = null;
        Process nextProcess = null;

        try {
            // 见执行起始命令
            currentProcess = run(commandId, startCommand, envp, dir);
            // 用于管道的字节输出流
            BufferedOutputStream out = null;
            // 用于标准错误输出和最终结果的字符输入流
            BufferedReader reader = null;
            // 用于管道的字节输入流
            BufferedInputStream in = null;
            // 用于管道的IO缓冲
            byte[] buffer = new byte[1024];
            try {
                // 遍历后续命令
//                for (String[] command : commands) {
//                    try {
//                        // 获取当前命令的标准错误流
//                        reader = new BufferedReader(new InputStreamReader(currentProcess.getErrorStream()));
//
//                        // 输出错误
//                        for (String temp = readLine(reader); temp != null; temp = readLine(reader)) {
//                            log.warn("Command(" + commandId + ") : " + temp);
//                        }
//                        // 获取当前命令的标准输出流
//                        in = new BufferedInputStream(currentProcess.getInputStream());
//                        // 启动下一条命令
//                        nextProcess = run(commandId, command, envp, dir);
//                        // 获取下一条命令的标准输入流
//                        out = new BufferedOutputStream(nextProcess.getOutputStream());
//
//                        // 管道的实现
//                        for (int c = read(in, buffer); c >= 0; c = read(in, buffer)) {
//                            write(out, buffer, c);
//                        }
//
//                        // 当前命令全部输出都已经输入到下一条命令中后，将下一条命令作为当前命令
//                        currentProcess = nextProcess;
//                    } finally { // 流关闭操作
//                        if (out != null) {
//                            out.flush();
//                            out.close();
//                            out = null;
//                        }
//
//                        if (in != null) {
//                            in.close();
//                            in = null;
//                        }
//
//                        if (reader != null) {
//                            reader.close();
//                            reader = null;
//                        }
//                    }
//                }

                // 获取最终命令的标准错误流，
                reader = new BufferedReader(new InputStreamReader(currentProcess.getErrorStream()));
                // 输出错误
                for (String temp = readLine(reader); temp != null; temp = readLine(reader)) {
                    log.warn("Command(" + commandId + ") : " + temp);
                }
                // 关闭
                reader.close();
                reader = null;

                // 获取最终命令的标准输出流
                reader = new BufferedReader(new InputStreamReader(currentProcess.getInputStream()));
                // 将输出添加到结果缓冲中
                for (String temp = reader.readLine(); temp != null; temp = reader.readLine()) {
                    result.append(temp + "\n");
                }
            } finally { // 流关闭操作
                if (out != null) {
                    out.flush();
                    out.close();
                    out = null;
                }

                if (in != null) {
                    in.close();
                    in = null;
                }

                if (reader != null) {
                    reader.close();
                    reader = null;
                }
            }
        } catch (Exception e) { // 出现异常，尝试把启动的进程都销毁
            log.warn("Command(" + commandId + ") Error", e);

            if (currentProcess != null) {
                currentProcess.destroy();
                currentProcess = null;
            }

            if (nextProcess != null) {
                nextProcess.destroy();
                nextProcess = null;
            }
        }

        log.info("Command(" + commandId + ") end");
        // 用结果缓冲构建StringReader
        return new StringReader(result.toString());
    }

    /**
     * 读取StringReader为String
     * @param stringReader     StringReader
     * @param skipWhiteLine     是否需要跳过空行
     * @return
     */
    public static String readStringReader(StringReader stringReader, boolean skipWhiteLine) {
        StringBuffer result = new StringBuffer();
        BufferedReader reader = new BufferedReader(stringReader);

        for (String temp = readLine(reader); temp != null; temp = readLine(reader)) {
            if (skipWhiteLine == false || !temp.trim().equals("")) {
                result.append(temp + "\n");
            }
        }

        return result.toString();

    }

    /**
     * 运行一个命令
     * @param commandId     命令id, 日志区别用
     * @param command     执行的命令
     * @param envp     运行的上下文环境变量，每项都应该写成name=value的格式；null表示直接继承当前Java进程的全部环境变量
     * @param dir     命令执行的工作目录；null表示继承当前Java进程的工作目录
     * @return 命令对应的Process对象Process
     * @throws Exception
     */
    private static Process run(String commandId, String[] command, String[] envp, File dir) throws Exception {
        log.info("Command(" + commandId + ") exec: " + Arrays.toString(command));

        return runtime.exec(command, envp, dir);
    }

    /**
     * 从输入流中读取数据进入buffer中，忽略异常
     * @param in     输入流
     * @param buffer     缓冲
     * @return 基本等同于in.read(buffer)的返回值，不过在出现异常时，不会抛出异常而是返回-1
     */
    private static int read(InputStream in, byte[] buffer) {
        int result = -1;

        try {
            result = in.read(buffer);
        } catch (IOException e) {
            result = -1;
        }

        return result;
    }

    /**
     * 从输入流中读取一行字符串，忽略异常
     * @param reader     输入流
     * @return 基本等同于reader.readLine()的返回值，不过在出现异常时，不会抛出异常而是返回null
     */
    private static String readLine(BufferedReader reader) {
        String result = null;

        try {
            result = reader.readLine();
        } catch (IOException e) {
            result = null;
        }

        return result;
    }

    /**
     * 向输出流写入buffer从0~length位置的数据，忽略异常
     * @param out     输出流
     * @param buffer     缓冲
     * @param length     最大位序号
     */
    private static void write(OutputStream out, byte[] buffer, int length) {
        try {
            out.write(buffer, 0, length);
        } catch (IOException e) {
        }
    }

    /**
     * 非阻塞模式回调
     */
    public static interface RuntimeCallback {
        /**
         * 非阻塞模式回调函数
         * @param result         命令执行结果
         * @param startCommand  起始命令
         * @param commands  其他后续命令，如果有设置，会使用管道来关联前后命令的标准输出流和标准输入流
         */
        public void onExit(StringReader result, String[] envp, File dir, String[] startCommand, String[]... commands);
    }

    public static void main(String[] args) {
        // 例子 ps -ef (注意用String[]方式来表示命令是不需要做转移的)
        System.out.println(readStringReader(run(null, null, new String[] { "ps", "-ef" }), true));
        System.out.println("-----------------------------------------------------------------");
        // 例子 ps -ef | grep -i java
        System.out.println(readStringReader(
                run(null, null, new String[] { "ps", "-ef" }, new String[] { "grep", "-i", "java" }), true));
    }
}


