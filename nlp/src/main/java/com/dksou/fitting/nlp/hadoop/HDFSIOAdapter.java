/*
 * <summary></summary>
 * <author>Hankcs</author>
 * <email>me@hankcs.com</email>
 * <create-date>2016-09-07 PM7:03</create-date>
 *
 * <copyright file="HDFSIOAdapter.java" company="码农场">
 * Copyright (c) 2008-2016, 码农场. All Right Reserved, http://www.hankcs.com/
 * This source is subject to Hankcs. Please contact Hankcs to get more information.
 * </copyright>
 */
package com.dksou.fitting.nlp.hadoop;

import com.hankcs.hanlp.corpus.io.IIOAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.hankcs.hanlp.utility.Predefine.logger;

/**
 * @author hankcs
 */
public class HDFSIOAdapter implements IIOAdapter
{

    private FileSystem fileSystem;

    public HDFSIOAdapter(FileSystem fileSystem)
    {
        this.fileSystem = fileSystem;
    }


    public HDFSIOAdapter(Configuration configuration)
    {
        try
        {
            fileSystem = FileSystem.get(configuration);
        }
        catch (IOException e)
        {
            logger.warning("获取Hadoop FileSystem失败");
            throw new IllegalArgumentException(e);
        }
    }

    public InputStream open(String path) throws IOException
    {
        Path hdfsPath = new Path(path);
        if (!fileSystem.exists(hdfsPath.getParent()))
            fileSystem.mkdirs(hdfsPath.getParent());
        return fileSystem.open(hdfsPath);
    }

    public OutputStream create(String path) throws IOException
    {
        Path hdfsPath = new Path(path);
        if (!fileSystem.exists(hdfsPath.getParent()))
            fileSystem.mkdirs(hdfsPath.getParent());
        return fileSystem.create(hdfsPath);
    }
}