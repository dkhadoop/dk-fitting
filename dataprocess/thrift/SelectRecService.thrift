namespace java com.dksou.fitting.dataprocess.service
  
service  SelectRecService  {  
  /**
   * 调用此方法可以筛选出符合条件的记录条数。
   * @param spStr分隔符号;
   * @param fdSum：字段数量;
   * @param whereStr：比较条件  f1 >= 2 and (f2=3 or f3=4)，f1为第一个字段;
   * @param srcDirName：源目录名;
   * @param dstDirName输出目录名，输出目录如果存在将会覆盖;
   * @param hostIp：要连接hiveserver主机的ip地址;
   * @param hostPort：hiveserver的端口，默认10000;
   * @param hostName：要连接主机的用户名;
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）;
   */
  void selectRec(1:string  spStr, 2:i32 fdSum, 3:string  whereStr,4:string  srcDirName, 5:string  dstDirName, 6:string  hostIp,7:string  hostPort, 8:string  hostName, 9:string  hostPassword)
}


