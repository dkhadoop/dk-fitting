namespace java com.dksou.fitting.dataprocess.service
  
service  FormatFieldService  {  
  /**
   * 调用此方法可以按关键字过滤出想要的字段。
   * @param spStr分隔符号;
   * @param fdSum：字段数量;
   * @param fdNum：字段序号（检查哪个字段是否符合正则，0为全部检查），可为一个或多个，多个之间用逗号分隔（1,2,3...）;
   * @param regExStr：字段中包含该字符的记录将被剔除（a,b,c），与字段序号相对应，多个字段时每个字段都符合该条件的记录将被剔除;
   * @param srcDirName：源目录名;
   * @param dstDirName输出目录名，输出目录如果存在将会覆盖;
   * @param hostIp：要连接hiveserver主机的ip地址;
   * @param hostPort：hiveserver的端口，默认10000;
   * @param hostName：要连接主机的用户名;
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）
   */
  void formatField(1:string  spStr, 2:i32 fdSum, 3:string  fdNum,4:string  regExStr, 5:string  srcDirName, 6:string  dstDirName,7:string  hostIp, 8:string  hostPort, 9:string  hostName, 10:string  hostPassword)
}