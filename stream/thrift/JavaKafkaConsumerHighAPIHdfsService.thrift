namespace java com.dksou.fitting.stream.service
  
service  JavaKafkaConsumerHighAPIHdfsService  {  
   
   /**
    *启动消费数据往hdfs上写入数据,参数在配置文件中配置
    *
    */
   string StartHdfs();

}