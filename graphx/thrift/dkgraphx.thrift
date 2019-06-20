namespace java com.dksou.fitting.graphx.service
typedef i32 int // We can use typedef to get pretty names for the types we are using

/**
    eg:
    1: DRIVER_MEMORY  driver 使用的内存，不可超过单机的内存总数 0g表示使用默认值 。
    2: NUM_EXECUTORS	创建多少个 executor , 0表示使用默认值。
    3: EXECUTOR_MEMORY 	各个 executor 使用的最大内存，不可超过单机的最大可使用内存，0g表示使用默认值。
    4: EXECUTOR_CORES 	各个 executor 使用的并发线程数目，也即每个 executor 最大可并发执行的 Task 数目，0表示使用默认值。
    5: QUEUE 在on yarn模式中指定使用队列的名称，default表示使用默认队列;
    6: PRINCIPAL 指定主体名称，default表示不使用主体;
    7: KEYTAB 使用keytab文件的地址，default表示不使用;
    eg:
        1: DRIVER_MEMORY = "1g";
        2: NUM_EXECUTORS = "2";
        3: EXECUTOR_MEMORY = "2g";
        4: EXECUTOR_CORES = "2";
        5: QUEUE = "default";
        6: PRINCIPAL = "default";
        7: KEYTAB = "default";
*/
struct DKGraphxConf {
    1: optional string DRIVER_MEMORY = "0g";
    2: optional string NUM_EXECUTORS = "0";
    3: optional string EXECUTOR_MEMORY = "0g";
    4: optional string EXECUTOR_CORES = "0";
    5: optional string QUEUE = "default";
    6: optional string PRINCIPAL = "default";
    7: optional string KEYTAB = "default";
}


service DKGraphx
{

    /**
     * 主要用于计算网页等级(链接多的网页)或者某社区中人物的重要性(关注比较多的人)。
     * @param masterUrl local[*]（spark的一种运行模式）。 或spark://IP:PORT
     * @param inputPath 数据所在路径（用户ID或网址）
     * @param delimiter 分隔符
     * @param dataType 数据类型，num：数值型，str：字符串
     * @param outputPath 结果输出目录
     * @param dkgraphxconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     * @throws TException
     */
    void pageRank(1:string masterUrl, 2:string inputPath, 3:string delimiter, 4:string dataType, 5:string outputPath,6:DKGraphxConf dkgraphxconf);


    /**
     * 主要用于社区中的人群分类，可根据人群的类别进行广告推送等。
     * @param masterUrl local[*], 或spark://IP:PORT
     * @param inputPath 数据所在路径
     * @param delimiter 分隔符
     * @param outputPath 结果输出目录
     * @param dkgraphxconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     * @throws TException
     */
    void commDetect(1:string masterUrl, 2:string inputPath, 3:string delimiter, 4:string outputPath,6:DKGraphxConf dkgraphxconf);


    /**
     * 主要用于发现社区中好友的好友，进行好友推荐。
     * 该方法对内存要求较大，建议每台节点至少64G。
     * 数据仅支持数值型。
     * @param masterUrl spark://IP:PORT --executor-memory=8G --driver-memory=1G
     * 建议executor-memory至少为8G
     * @param inputPath 数据所在路径
     * @param delimiter 分隔符
     * @param outputPath 结果输出目录
     * @param dkgraphxconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     * @throws TException
     */
    void frindsFind(1:string masterUrl, 2:string inputPath, 3:string delimiter, 4:string outputPath,6:DKGraphxConf dkgraphxconf);

}