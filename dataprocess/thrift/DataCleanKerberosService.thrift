namespace java com.dksou.fitting.dataprocess.service
  
service  DataCleanKerberosService  {  
   /**
    * 该方法可用于对数据条件筛选分析或分组统计分析
    * @param spStr：分隔符号
    * @param fdSum：字段数量
    * @param whereStr：筛选条件，如："\"f1='T100'\""，若无请写1=1
    * @param groupStr：分组条件，如："f1"，若无请写1
    * @param srcDirName：文件所在目录
    * @param dstDirName：数据所在目录
    * @param hostIp：要连接hiveserver主机的ip地址，
    * @param hostPort：hiveserver的端口，默认10000
    * @param hostName：要连接主机的用户名，
    * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）
    * @param user：Service Principal登陆用户名;
    * @param krb5Path：krb5.conf存放路径
    * @param keytabPath：hive.keytab存放路径
    * @param principalPath：hive服务所对应的principal 例: principal=hive/cdh166@EXAMPLE.COM
    */
  void analyseKerberos(1:string  spStr,2:i32 fdSum,3:string  whereStr,4:string  groupStr,5:string  srcDirName,6:string  dstDirName,7:string  hostIp,8:string  hostPort,9:string  hostName,10:string  hostPassword,11:string user,12:string krb5Path,13:string keytabPath,14:string principalPath)
  /**
   * 该方法可分析某两种物品同时出现的频率。
   * @param spStr：分隔符号
   * @param fdSum：字段数量
   * @param pNum：要分析的物品所在字段
   * @param oNum：订单号等所在字段
   * @param whereStr：筛选条件，如："\"f1='T100'\""，若无请写1=1
   * @param srcDirName：文件所在目录
   * @param dstDirName：数据所在目录
   * @param hostIp：要连接hiveserver主机的ip地址，
   * @param hostPort：hiveserver的端口，默认10000
   * @param hostName：要连接主机的用户名，
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）
   * @param user：Service Principal登陆用户名;
   * @param krb5Path：krb5.conf存放路径
   * @param keytabPath：hive.keytab存放路径
   * @param principalPath：hive服务所对应的principal 例: principal=hive/cdh166@EXAMPLE.COM
   */
  void apriori2Kerberos(1:string  spStr, 2:i32 fdSum,3:string  pNum,4:string  oNum,5:string  whereStr,6:string  srcDirName,7:string  dstDirName,8:string  hostIp,9:string  hostPort,10:string  hostName,11:string  hostPassword, 12:string user,13:string krb5Path,14:string keytabPath,15:string principalPath)
  /**
   * 该方法可分析某三种物品同时出现的频率
   * @param spStr：分隔符号
   * @param fdSum：字段数量
   * @param pNum：要分析的物品所在字段
   * @param oNum：订单号等所在字段
   * @param whereStr：筛选条件，如："\"f1='T100'\""，若无请写1=1
   * @param srcDirName：文件所在目录
   * @param dstDirName：数据所在目录
   * @param hostIp：要连接hiveserver主机的ip地址，
   * @param hostPort：hiveserver的端口，默认10000
   * @param hostName：要连接主机的用户名，
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）
   * @param user：Service Principal登陆用户名;
   * @param krb5Path：krb5.conf存放路径
   * @param keytabPath：hive.keytab存放路径
   * @param principalPath：hive服务所对应的principal
   */
  void apriori3Kerberos(1:string  spStr,2:i32 fdSum,3:string  pNum,4:string  oNum,5:string  whereStr,6:string  srcDirName,7:string  dstDirName,8:string  hostIp,9:string  hostPort, 10:string  hostName, 11:string  hostPassword,12:string user,13:string krb5Path,14:string keytabPath,15:string principalPath)
  /**
   * 调用此方法可以筛选出符合条件的记录条数
   * @param spStr分隔符号;
   * @param fdSum：字段数量;
   * @param whereStr：比较条件  f1 >= 2 and (f2=3 or f3=4)，f1为第一个字段;
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
  void selectRecKerberos(1:string  spStr, 2:i32 fdSum, 3:string  whereStr,4:string  srcDirName, 5:string  dstDirName, 6:string  hostIp,7:string  hostPort, 8:string  hostName, 9:string  hostPassword ,10:string user,11:string krb5Path,12:string keytabPath,13:string principalPath)
  /**
   * 调用此方法可以按关键字过滤出想要的字段
   * @param spStr分隔符号;
   * @param fdSum：字段数量;
   * @param fdNum：字段序号（检查哪个字段是否符合正则，0为全部检查），可为一个或多个，多个之间用逗号分隔（1,2,3...）;
   * @param regExStr：字段中包含该字符的记录将被剔除（a,b,c），与字段序号相对应，多个字段时每个字段都符合该条件的记录将被剔除;
   * @param srcDirName：源目录名;
   * @param dstDirName输出目录名，输出目录如果存在将会覆盖;
   * @param hostIp：要连接hiveserver主机的ip地址;
   * @param hostPort：hiveserver的端口，默认10000;
   * @param hostName：要连接主机的用户名;
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）;
   * @param user：Service Principal登陆用户名;
   * @param krb5Path：krb5.conf存放路径
   * @param keytabPath：hive.keytab存放路径
   * @param principalPath：hive服务所对应的principal
   */ 
  void formatFieldKerberos(1:string  spStr, 2:i32 fdSum, 3:string  fdNum,4:string  regExStr, 5:string  srcDirName, 6:string  dstDirName,7:string  hostIp, 8:string  hostPort, 9:string  hostName, 10:string  hostPassword,11:string user,12:string krb5Path,13:string keytabPath,14:string principalPath)
  /**
   * 调用此方法可以将不合法的记录去除掉
   * @param spStr分隔符号，
   * @param fdSum：字段数量（不符合该数量的记录将被清除），
   * @param srcDirName：源目录名，
   * @param dstDirName输出目录名，输出目录如果存在将会覆盖
   * @param hostIp：要连接hiveserver主机的ip地址.
   * @param hostPort：hiveserver的端口，默认10000
   * @param hostName：要连接主机的用户名，
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）。
   * @param fittingHome：freeHome的目录
   * @param javaBinHome：Java的bin的全路径
   * @param user：Service Principal登陆用户名;
   * @param krb5Path：krb5.conf存放路径
   * @param keytabPath：hive.keytab存放路径
   * @param principalPath：hive服务所对应的principal
   */
  void formatRecKerberos(1:string spStr, 2:i32 fdSum, 3:string srcDirName, 4:string dstDirName, 5:string hostIp, 6:string hostPort, 7:string hostName, 8:string hostPassword,9:string user,10:string krb5Path,11:string keytabPath,12:string principalPath)
  /**
   * 调用此方法可以从所有字段中筛选出想要的几个字段数据
   * @param spStr：分隔符号;
   * @param fdSum：字段数量;
   * @param fdNum：字段数组（整数数组，内容是要保留的字段序号，没有编号的字段将去除），输入格式：逗号分隔的数字（1,2,3...）; 
   * @param srcDirName：源目录名;
   * @param dstDirName：输出目录名，输出目录如果存在将会覆盖;
   * @param hostIp：要连接hiveserver主机的ip地址;
   * @param hostPort：hiveserver的端口，默认10000;
   * @param hostName：要连接主机的用户名;
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）;
   * @param user：Service Principal登陆用户名;
   * @param krb5Path：krb5.conf存放路径
   * @param keytabPath：hive.keytab存放路径
   * @param principalPath：hive服务所对应的principal 例: principal=hive/cdh166@EXAMPLE.COM
   */
  void selectFieldKerberos(1:string  spStr, 2:i32 fdSum, 3:string  fdNum,4:string  srcDirName, 5:string  dstDirName, 6:string  hostIp,7:string  hostPort, 8:string  hostName, 9:string  hostPassword,10:string user,11:string krb5Path,12:string keytabPath,13:string principalPath)

}