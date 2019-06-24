namespace java com.dksou.fitting.dataprocess.service
  
service  DedupeKerberosService  {
   /**
    * 该方法可筛选出不同的数据或字段
    * @param spStr分隔符号;
    * @param fdNum：字段数组（去重的字段，0为整条记录，输入格式：0或逗号分隔的数字（1,2,3...）;
    * @param srcDirName：源目录名;
    * @param dstDirName输出目录名，输出目录如果存在将会覆盖;
    * @param hostIp：要连接hiveserver主机的ip地址;
    * @param hostPort：hiveserver的端口，默认10000;
    * @param hostName：要连接主机的用户名;
    * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）;
    * @param user：Service Principal登陆用户名;
    * @param krb5Path：krb5.conf存放路径
    * @param keytabPath：hive.keytab存放路径
    * @param principalPath：hive服务所对应的principal 例: principal=hive/cdh166@EXAMPLE.COM
    *
    */
  void dedupKerberos(1:string  spStr,2:string  fdNum,3:string  srcDirName,4:string  dstDirName,5:string  hostIp,6:string  hostPort,7:string  hostName,8:string  hostPassword,9:string user,10:string krb5Path,11:string keytabPath,12:string principalPath)  
}