package com.dksou.fitting.datasource.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Properties;

public class HdfsUtils {
    private static FileSystem fs = null;
    private static Configuration conf = new Configuration();
    private static Properties Prop = PropUtil.loadProp(System.getProperty("user.dir") + "/conf/datasource.properties");


    public static FileSystem getFs(String krb5_conf, String principal, String keytab) throws Exception {
        if (fs != null) {
            return fs;
        } else {
            System.out.println("hdfs_site:" + Prop.getProperty("datasource.hdfs_xml_path"));
            System.out.println("core_site:" + Prop.getProperty("datasource.core_xml_path"));
            conf.addResource(new Path(Prop.getProperty("datasource.hdfs_xml_path")));
            conf.addResource(new Path(Prop.getProperty("datasource.core_xml_path")));
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            //conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            if (StringUtils.isNotBlank(krb5_conf) && StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(keytab)) {
                System.setProperty("java.security.krb5.conf", krb5_conf);
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
                UserGroupInformation.getLoginUser();
            }
            fs = FileSystem.get(conf);
            return fs;
        }
    }

}
