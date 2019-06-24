namespace java com.dksou.fitting.stream.service
  
service  DKStreamProducerService  {  
  
  /**
   * 创建kafka队列
   * @param topicName 队列名称
   * @param partitions 分区数量
   * @param replicationFactor topic中数据的副本数量
   *
   */
  void createTopic(1:string topicName, 2:i32 partitions, 3:i32 replicationFactor)
  /**
   * 删除一个topic，这个删除只是告知系统标记该topic要被删除。而不是立即删除。
   * @param topicName topic的名字
   */
  void deleteTopic(1:string topicName)
  /**
   * 判断一个topic是否已经存在。 包括已经被标记删除，还没有删除的topic。
   * @param topicName topic的名字
   * @return
   */
  bool topicIsExists(1:string topicName)
  /**
   * 查询所有topic，包括已经被标记删除，还没有删除的topic。
   * @return topic的list
   */
  list<string> queryAllTopic()
  /**
   * 发送一条数据到指定topic中。
   * @param topicName topic的名称
   * @param key  键值 。 没有则填null
   * @param message 要发送的消息
   */
  void streamDataToTopic(1:string topicName, 2:string key, 3:string message);
}