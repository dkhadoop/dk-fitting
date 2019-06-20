package com.dksou.fitting.datasource.util;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.dksou.fitting.datasource.service.ResultEntity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SshUtil {
    public static String scp(String hostIp, String hostName,
                             String hostPassword, String localFile, String remotePath) throws Exception {
        Connection conn = createConn(hostIp, hostName, hostPassword);

        SCPClient scpClient = new SCPClient(conn);
        scpClient.put(localFile, remotePath, "0777");
        return "upLoad success";
    }

    static Connection createConn(String hostIp, String hostName,
                                 String hostPassword) throws Exception {
        Connection conn = new Connection(hostIp);
        conn.connect();
        boolean isAuthenticated = conn.authenticateWithPassword(hostName, hostPassword);

        if (isAuthenticated == false)
            throw new IOException("Authentication failed.");
        return conn;
    }


    public static String exe(String cmd, String hostIp, String hostName,
                             String hostPassword) throws Exception {
        Connection conn = createConn(hostIp, hostName, hostPassword);
        Session sess = conn.openSession();
        sess.requestPTY("vt100", 80, 24, 640, 480, null);
        sess.execCommand(cmd);
        InputStream stdout = new StreamGobbler(sess.getStdout());
        StringBuilder sb = new StringBuilder();
        BufferedReader stdoutReader = new BufferedReader(
                new InputStreamReader(stdout));

        System.out.println("Here is the output from stdout:");
        while (true) {
            String line = stdoutReader.readLine();
            if (line == null)
                break;
            System.out.println(line);
            sb.append(line + "\n");
        }

//      System.out.println("ExitCode: " + sess.getExitStatus());
//		System.out.println("sb.toString() = " + sb.toString());
        sess.close();
        conn.close();
        return sb.toString();
    }

    public static ResultEntity exe(ResultEntity rs, String cmd) {
        StringBuffer sb = new StringBuffer();
        Process ps = null;
        try {
            String[] commd = new String[]{"/bin/sh", "-c", cmd};
            ps = Runtime.getRuntime().exec(commd);
            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(ps.getErrorStream()));
            String line;
            System.out.println("Here is the output from stdout:");
            while ((line = stdoutReader.readLine()) != null) {
                System.out.println(line);
                sb.append(line).append("\n");
            }
            int status = ps.waitFor();
            if (status != 0) {
                sb.append("Here is the output from stderr: \n");
                while ((line = stderrReader.readLine()) != null) {
                    System.out.println(line);
                    sb.append(line).append("\n");
                }
                rs.setMessage(sb.toString());
            } else {
                rs.setStatus(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } catch (InterruptedException e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (ps != null) {
                ps.destroy();
            }
        }
        return rs;
    }

    public static void exe(String cmd) {
        StringBuffer sb = new StringBuffer();
        Process ps = null;
        try {
            String[] commd = new String[]{"/bin/sh", "-c", cmd};
            ps = Runtime.getRuntime().exec(commd);
            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(ps.getErrorStream()));
            String line;
            System.out.println("Here is the output from stdout:");
            while ((line = stdoutReader.readLine()) != null) {
                System.out.println(line);
                sb.append(line).append("\n");
            }
            int status = ps.waitFor();
            if (status != 0) {
                sb.append("Here is the output from stderr: \n");
                while ((line = stderrReader.readLine()) != null) {
                    System.out.println(line);
                    sb.append(line).append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } finally {
            if (ps != null) {
                ps.destroy();
            }
        }
    }


    public static String getHomePath(String cmd, String hostIp, String hostName,
                                     String hostPassword, String hadoopName) throws Exception {
        Connection conn = createConn(hostIp, hostName, hostPassword);
        Session sess = conn.openSession();
        sess.requestPTY("vt100", 80, 24, 640, 480, null);
        sess.execCommand(cmd);
        InputStream stdout = new StreamGobbler(sess.getStdout());
        StringBuilder sb = new StringBuilder();
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));

//		System.out.println("Here is the output from stdout:");
        String hadoopNamePath = "";
        while (true) {
            String line = stdoutReader.readLine();
//			System.out.println( trim);
            if (line != null && line.trim().startsWith("export " + hadoopName)) {
                String substring = line.substring(line.lastIndexOf("=") + 1);
                hadoopNamePath = substring;
            }
            if (line == null)
                break;
        }

        //System.out.println("ExitCode: " + sess.getExitStatus());
//		System.out.println("sb.toString() = " + sb.toString());
        sess.close();
        conn.close();
        return hadoopNamePath;
    }
}
