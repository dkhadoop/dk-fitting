package com.dksou.fitting.datasource.service.serviceimpl;

import com.dksou.fitting.datasource.service.DKFileOutput;
import com.dksou.fitting.datasource.service.DKFILEConf;
import com.dksou.fitting.datasource.service.FileData;
import com.dksou.fitting.datasource.service.ResultEntity;
import com.dksou.fitting.datasource.util.HdfsUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.*;

public class DKFileOutputImpl implements DKFileOutput.Iface {
    private static Logger logger = Logger.getLogger(DKFileOutputImpl.class);

    /**
     * @param exportDir  hdfs下载的文件名（路径+文件名全称）
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
    @Override
    public FileData hdfsToFileByBinary(String exportDir, DKFILEConf dkFileConf) {
        return getFileData(exportDir, dkFileConf);
    }

    /**
     * @param filePath   为本地文件目录名
     * @param exportDir  要从hdfs导出的目录
     * @param dkFileConf dkFile服务的配置项：参考说明文档
     * @return
     * @throws TException
     */
    @Override
    public ResultEntity hdfsToFiles(String filePath, String exportDir, DKFILEConf dkFileConf) {
        ResultEntity rs = new ResultEntity();
        FileSystem fs = null;
        FSDataInputStream fsDataInputStream = null;
        BufferedOutputStream bos = null;
        if (!filePath.endsWith("\\") && !filePath.endsWith("/")) {
            filePath += "/";
        }
        try {
            fs = HdfsUtils.getFs(dkFileConf.KRB5_CONF, dkFileConf.PRINCIPAL, dkFileConf.KEYTAB);
            RemoteIterator listFilesHdfs = fs.listFiles(new Path(exportDir), true);
            while (listFilesHdfs.hasNext()) {
                LocatedFileStatus next = (LocatedFileStatus) listFilesHdfs.next();
                Path path = next.getPath();
                if (fs.isFile(path)) {
                    fsDataInputStream = fs.open(path, 2048);
                    File dirFile = new File(filePath);
                    dirFile.mkdirs();
                    bos = new BufferedOutputStream(new FileOutputStream(new File(filePath + path.getName())));
                    IOUtils.copyBytes(fsDataInputStream, bos, 2048, true);
                } else {
                    File folder = new File(path.toString());
                    if (!(folder.exists()))
                        folder.mkdir();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            rs.setMessage("File download failed:" + e.getMessage());
            return rs;
        } finally {
            if (fsDataInputStream != null) {
                try {
                    fsDataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    rs.setMessage(e.getMessage());
                    return rs;
                }
            }
            if (bos != null) {
                try {
                    bos.close();
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

    private FileData getFileData(String filePath, DKFILEConf dkFileConf) {
        FileSystem fs = null;
        FSDataInputStream fsDataInputStream = null;
        byte[] buffer = null;
        FileData fileData = new FileData();
        fileData.setStatus(0);
        fileData.setMessage("执行成功!");
        try {
            fs = HdfsUtils.getFs(dkFileConf.KRB5_CONF, dkFileConf.PRINCIPAL, dkFileConf.KEYTAB);
            Path path = new Path(filePath);
            if (fs.exists(path)) {
                fileData.setName(path.getName());
                fsDataInputStream = fs.open(path);
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
            byte[] b = new byte[1000];
            int n;
            while ((n = fsDataInputStream.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            if (fsDataInputStream != null)
                fsDataInputStream.close();
            bos.close();
            buffer = bos.toByteArray();
            fileData.setBuff(buffer);

        } catch (Exception e) {
            fileData.setStatus(-1);
            fileData.setMessage("File download failed：" + e.getMessage());
            e.printStackTrace();
        }
        return fileData;
    }
}
