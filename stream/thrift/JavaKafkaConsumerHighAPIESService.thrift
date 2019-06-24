namespace java com.dksou.fitting.stream.service
  
service  JavaKafkaConsumerHighAPIESService  {  

   /**
    *启动消费数据往ES上写入数据,参数在配置文件中配置
    *
    */
   string StartES();

}