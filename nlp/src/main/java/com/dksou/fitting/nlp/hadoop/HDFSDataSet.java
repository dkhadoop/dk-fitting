/*
 * <summary></summary>
 * <author>Hankcs</author>
 * <email>me@hankcs.com</email>
 * <create-date>2016-09-07 PM10:23</create-date>
 *
 * <copyright file="HDFSDataSet.java" company="码农场">
 * Copyright (c) 2008-2016, 码农场. All Right Reserved, http://www.hankcs.com/
 * This source is subject to Hankcs. Please contact Hankcs to get more information.
 * </copyright>
 */
package com.dksou.fitting.nlp.hadoop;

import com.hankcs.hanlp.corpus.io.IOUtil;
import com.hankcs.hanlp.utility.Predefine;
import com.hankcs.nlp.corpus.IDataSet;
import com.hankcs.nlp.corpus.MemoryDataSet;
import com.hankcs.nlp.utilities.MathUtility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.hankcs.nlp.utilities.Predefine.logger;

/**
 * @author hankcs
 */
public class HDFSDataSet extends MemoryDataSet
{
    private FileSystem fileSystem;

    public HDFSDataSet(FileSystem fileSystem)
    {
        this.fileSystem = fileSystem;
    }


    public HDFSDataSet(Configuration configuration)
    {
        try
        {
            fileSystem = FileSystem.get(configuration);
        }
        catch (IOException e)
        {
            Predefine.logger.warning("获取Hadoop FileSystem失败");
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public IDataSet load(String folderPath, String charsetName, double rate) throws IllegalArgumentException, IOException
    {
        if (folderPath == null) throw new IllegalArgumentException("参数 folderPath == null");
        Path root = new Path(folderPath);
        if (!fileSystem.exists(root)) throw new IllegalArgumentException(String.format("目录 %s 不存在", root.getName()));
        if (!fileSystem.isDirectory(root))
            throw new IllegalArgumentException(String.format("目录 %s 不是一个目录", root.getName()));
        if (rate > 1.0 || rate < -1.0) throw new IllegalArgumentException("rate 的绝对值必须介于[0, 1]之间");

        FileStatus[] folders = fileSystem.listStatus(root);
        if (folders == null) return null;
        logger.start("模式:%s\n文本编码:%s\n根目录:%s\n加载中...\n", isTestingDataSet() ? "测试集" : "训练集", charsetName, folderPath);
        for (FileStatus fs : folders)
        {
            if (fs.isFile()) continue;
            Path folder = fs.getPath();
            FileStatus[] files = fileSystem.listStatus(folder);
            if (files == null) continue;
            String category = folder.getName();
            logger.out("[%s]...", category);
            int b, e;
            if (rate > 0)
            {
                b = 0;
                e = (int) (files.length * rate);
            }
            else
            {
                b = (int) (files.length * (1 + rate));
                e = files.length;
            }

            for (int i = b; i < e; i++)
            {
                String path = files[i].getPath().getName();
                if (!path.startsWith(folderPath))
                {
                    path = folderPath + "/" + folder.getName() + "/" + path;
                }
                add(folder.getName(), IOUtil.readTxt(path, charsetName));
                if (i % 100 == 0)
                    logger.out("%.2f%%...", MathUtility.percentage(i - b, e - b));
            }
            logger.out(" %d 篇文档\n", e - b);
        }
        logger.finish(" 加载了 %d 个类目,共 %d 篇文档\n", getCatalog().size(), size());
        return this;
    }
}
