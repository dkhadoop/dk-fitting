package com.dksou.fitting.datasource.service.serviceimpl;

import com.dksou.fitting.datasource.service.DKFileInput;
import com.dksou.fitting.datasource.service.DKFILEConf;
import com.dksou.fitting.datasource.service.ResultEntity;
import com.dksou.fitting.datasource.util.HdfsUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;

public class DKFileInputImpl implements DKFileInput.Iface {
    private static Logger logger = Logger.getLogger(DKFileInputImpl.class);
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     * @param fileName   文件名
     * @param fileData   文件内容
     * @param dirName    输出的hdfs路径（不包括文件名）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     */
    @Override
    public ResultEntity fileToHdfsByBinary(String fileName, ByteBuffer fileData, String dirName, DKFILEConf dkFileConf) {
        ResultEntity rs = new ResultEntity();
        FileSystem fs = null;
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fs = HdfsUtils.getFs(dkFileConf.KRB5_CONF, dkFileConf.PRINCIPAL, dkFileConf.KEYTAB);
            if (!dirName.endsWith("/")) {
                dirName = dirName + "/" + fileName;
            } else {
                dirName = dirName + fileName;
            }
            fsDataOutputStream = fs.create(new Path(dirName));
            fsDataOutputStream.write(fileData.array());
            fsDataOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (fsDataOutputStream != null) {
                try {
                    fsDataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        rs.setStatus(0);
        return rs;
    }

    /**
     * @param filePath   单个文件的绝对路径（包含文件名）
     * @param dirName    输出的hdfs路径（不包含文件名）
     * @param fileLength 文件长度限制（K为单位。）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
    @Override
    public ResultEntity fileToHdfs(String filePath, String dirName, int fileLength, DKFILEConf dkFileConf) {
        ResultEntity rs = new ResultEntity();
        FileSystem fs = null;
        FileInputStream fileInputStream = null;
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fs = HdfsUtils.getFs(dkFileConf.KRB5_CONF, dkFileConf.PRINCIPAL, dkFileConf.KEYTAB);
            File file = new File(filePath);
            long length = file.length() / 1024;
            boolean whether = whetherOutOfFileSize(fileLength, length);
            if (whether) {
                throw new RuntimeException("文件超过规定大小。 文件大小为： " + length + "KB");
            }
            fileInputStream = new FileInputStream(new File(filePath));
            if (dirName.endsWith("/")) {
                dirName = dirName + file.getName();
            } else {
                dirName = dirName + "/" + file.getName();
            }
            fsDataOutputStream = fs.create(new Path(dirName));
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 1024);

        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        } finally {
            if (fsDataOutputStream != null) {
                try {
                    fsDataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
        }
        rs.setStatus(0);
        return rs;
    }

    /**
     * @param filePath   多文件上传，参数为文件的父路径
     * @param dirName    输出到hdfs上的路径
     * @param fileLength 文件长度限制（K为单位。）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     */
    @Override
    public ResultEntity filesToHdfs(String filePath, String dirName, int fileLength, DKFILEConf dkFileConf) {
        ResultEntity rs = new ResultEntity();
        FileSystem fs = null;
        FileInputStream fileInputStream = null;
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fs = HdfsUtils.getFs(dkFileConf.KRB5_CONF, dkFileConf.PRINCIPAL, dkFileConf.KEYTAB);
            Collection<File> listFiles = FileUtils.listFiles(new File(filePath), null, true);
            for (File file : listFiles) {
                String absolutePath = file.getPath();
                long length = file.length() / 1024;
                boolean whether = whetherOutOfFileSize(fileLength, length);
                if (whether) {
                    logger.info(file.getPath() + "文件超过规定大小。 文件大小为： " + length + "KB 已跳过！");
                    continue;
                }
                if (!filePath.endsWith("/")) {
                    filePath = filePath + "/";
                }
                String replaceAbs = absolutePath.replace(filePath, "");
                if (!dirName.endsWith("/")) {
                    dirName = dirName + "/";
                }
                String dirFilePath = dirName + replaceAbs;
                fileInputStream = new FileInputStream(file);
                fsDataOutputStream = fs.create(new Path(dirFilePath));
                IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 2048, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage(e.getMessage());
            return rs;
        }
        rs.setStatus(0);
        return rs;
    }

    /**
     * @param fileLength 限制文件大小
     * @param length     实际文件大小
     * @return
     */
    private static boolean whetherOutOfFileSize(int fileLength, long length) {
        if (fileLength != 0) {
            if (fileLength < length) {
                return true;
            }
        }
        return false;
    }
}
