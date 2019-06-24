namespace java com.dksou.fitting.dataprocess.service
  
service  SelectFieldService  { 
  /**
   * 调用此方法可以从所有字段中筛选出想要的几个字段数据
   * @param spStr分隔符号;
   * @param fdSum：字段数量;
   * @param fdNum：字段数组（整数数组，内容是要保留的字段序号，没有编号的字段将去除），输入格式：逗号分隔的数字（1,2,3...）;
   * @param srcDirName：源目录名;
   * @param dstDirName输出目录名，输出目录如果存在将会覆盖;
   * @param hostIp：要连接hiveserver主机的ip地址;
   * @param hostPort：hiveserver的端口，默认10000;
   * @param hostName：要连接主机的用户名;
   * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）
   */
  void selectField(1:string  spStr, 2:i32 fdSum, 3:string  fdNum,4:string  srcDirName, 5:string  dstDirName, 6:string  hostIp,7:string  hostPort, 8:string  hostName, 9:string  hostPassword)
}



