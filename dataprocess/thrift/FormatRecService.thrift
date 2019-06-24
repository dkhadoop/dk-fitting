namespace java com.dksou.fitting.dataprocess.service
  
service  FormatRecService  {  
  /** 
   * 调用此方法可以将不合法的记录去除掉。
   * @param spStr分隔符号，
   * @param fdSum：字段数量（不符合该数量的记录将被清除），
   * @param srcDirName：源目录名，
   * @param dstDirName输出目录名，输出目录如果存在将会覆盖
   * @param hostIp：要连接hiveserver主机的ip地址.
   * @param hostPort：hiveserver的端口，默认10000
   * @param hostName：要连接主机的用户名，
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）。
   */
  void formatRec(1:string spStr, 2:i32 fdSum, 3:string srcDirName, 4:string dstDirName, 5:string hostIp, 6:string hostPort, 7:string hostName, 8:string hostPassword)
}

