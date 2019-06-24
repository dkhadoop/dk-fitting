namespace java com.dksou.fitting.dataprocess.service
  
service  DataStaticKerberosService  {  
  /**
   * 该方法可对某字段取最大值、最小值、求和、计算平均值。
   * @param fun：功能avg,min,max,sum，
   * @param fdSum：字段数量
   * @param spStr分隔符号，
   * @param fdNum：字段编号，
   * @param dirName：目录名
   * @param hostIp：要连接hiveserver主机的ip地址，
   * @param hostPort：hiveserver的端口，默认10000
   * @param hostName：要连接主机的用户名，
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）
   * @param user：Service Principal登陆用户名;
   * @param krb5Path：krb5.conf存放路径
   * @param keytabPath：hive.keytab存放路径
   * @param principalPath：hive服务所对应的principal
   */
  double countKerberos(1:string   fun, 2:i32 fdSum, 3:string   spStr, 4:i32 fdNum,5:string   dirName, 6:string   hostIp, 7:string   hostPort, 8:string   hostName, 9:string   hostPassword, 10:string user,11:string krb5Path,12:string keytabPath,13:string principalPath )
  /**
   * 该方法可计算某字段符合某条件的记录数。
   * @param fun：功能count
   * @param fdSum：字段数量
   * @param spStr分隔符号，
   * @param fdNum：字段编号，
   * @param @param compStr：比较符号，>, <, >=, <=, =,!=用法："'>='"
   * @param whereStr：比较条件
   * @param dirName：目录名
   * @param hostIp：要连接hiveserver主机的ip地址，
   * @param hostPort：hiveserver的端口，默认10000
   * @param hostName：要连接主机的用户名，
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）。
   * @param user：Service Principal登陆用户名;
   * @param krb5Path：krb5.conf存放路径
   * @param keytabPath：hive.keytab存放路径
   * @param principalPath：hive服务所对应的principal
   */
  double countRecordKerberos(1:string  fun, 2:i32 fdSum, 3:string  spStr, 4:i32 fdNum,5:string  compStr, 6:string  whereStr, 7:string  dirName, 8:string  hostIp, 9:string  hostPort,10:string  hostName, 11:string  hostPassword  12:string user,13:string krb5Path,14:string keytabPath,15:string principalPath)

}




