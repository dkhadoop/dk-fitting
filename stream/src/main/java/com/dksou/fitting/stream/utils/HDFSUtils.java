package com.dksou.fitting.stream.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;




public class HDFSUtils {

	//初始化fs
	public static FileSystem getFs(String hdfs_xml, String core_xml, String krb5_conf, String principal, String keytab) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		conf.addResource(new Path(hdfs_xml));
		conf.addResource(new Path(core_xml));
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		if (principal != null && !"".equals(principal) && keytab != null && !"".equals(keytab) && krb5_conf != null && !"".equals(krb5_conf)) {
			System.setProperty("java.security.krb5.conf", krb5_conf);
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(principal, keytab);
			UserGroupInformation.getLoginUser();
		}
		fs = FileSystem.get(conf);
		return fs;
	}


	/**
	 * 将consumer从topic中获取的数据写入到HDFS
	 * @param path: HDFS上的文件
	 * @param data：数据
	 */
	public synchronized static void sendToHDFS(String hdfs_xml, String core_xml,String krb5_conf, String principal, String keytab,String path,String data) throws Exception{
		FileSystem fs = getFs(hdfs_xml,core_xml,krb5_conf,principal,keytab);
		Path path1 = new Path(path);
		if(!fs.exists(path1)){
			//创建文件
			fs.create(path1).close();
		}
		final FSDataOutputStream append = fs.append(path1);
		//将数据写入到HDFS
		append.write(data.getBytes());
		append.close();

	}


	public static void main(String[] args) {

		String hdfs_xml = "/opt/hadoop-2.6.0/etc/hadoop/hdfs-site.xml";
		String core_xml = "/opt/hadoop-2.6.0/etc/hadoop/core-site.xml";
		String path = "/test/data";
		String data = "yitiaoxindeshuju";

		String krb5_conf = "";
		String principal = "";
		String keytab = "";

		try {
			sendToHDFS(hdfs_xml,core_xml,krb5_conf,principal,keytab,path,data);
		} catch (Exception e) {
			e.printStackTrace();
		}


	}



}









