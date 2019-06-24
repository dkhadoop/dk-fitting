namespace java com.dksou.fitting.dataprocess.service
  
service  DataAnalysisService  {  
  
  /**
    * 该方法可用于对数据条件筛选分析或分组统计分析
    * @param spStr：分隔符号
    * fdSum：字段数量
    * @param whereStr：筛选条件，如："\"f1='T100'\""，若无请写1=1
    * @param groupStr：分组条件，如："f1"，若无请写1
    * @param srcDirName：文件所在目录
    * @param dstDirName：数据所在目录
    * @param hostIp：要连接hiveserver主机的ip地址，
    * @param hostPort：hiveserver的端口，默认10000
    * @param hostName：要连接主机的用户名，
    * @param hostPassword：要连接主机的密码（要具备执行Hadoop的权限的用户）
    */
  void analyse(1:string  spStr,2:i32 fdSum,3:string  whereStr,4:string  groupStr,5:string  srcDirName,6:string  dstDirName,7:string  hostIp,8:string  hostPort,9:string  hostName,10:string  hostPassword)
  /**
    * 该方法可分析某两种物品同时出现的频率
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
    */
  void apriori2(1:string  spStr, 2:i32 fdSum,3:string  pNum,4:string  oNum,5:string  whereStr,6:string  srcDirName,7:string  dstDirName,8:string  hostIp,9:string  hostPort,10:string  hostName,11:string  hostPassword)
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
   */
  void apriori3(1:string  spStr,2:i32 fdSum,3:string  pNum,4:string  oNum,5:string  whereStr,6:string  srcDirName,7:string  dstDirName,8:string  hostIp,9:string  hostPort, 10:string  hostName, 11:string  hostPassword)
  
}