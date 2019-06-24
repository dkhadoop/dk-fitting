namespace java com.dksou.fitting.ml.service
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
struct DKMLConf {
    1: optional string DRIVER_MEMORY = "0g";
    2: optional string NUM_EXECUTORS = "0";
    3: optional string EXECUTOR_MEMORY = "0g";
    4: optional string EXECUTOR_CORES = "0";
    5: optional string QUEUE = "default";
    6: optional string PRINCIPAL = "default";
    7: optional string KEYTAB = "default";
}


service DKML
{       
    /**
	 * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClass 分类数目
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void lrModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:int numClass, 5:DKMLConf dkmlconf);

    /**
	 * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 结果保存路径
     * @param conf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void lrModelPredict(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

    /**
	 * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClass 分类数目
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void rfClassModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:int numClass, 5:DKMLConf dkmlconf);
	
    /**
	 * 构建回归模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void rfRegresModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 5:DKMLConf dkmlconf);

    /**
	 * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 结果保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void rfModelPredict(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

    /**
	 * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void svmModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 5:DKMLConf dkmlconf);

    /**
	 * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 结果保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void svmModelPredict(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

    /**
	 * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void nbModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 5:DKMLConf dkmlconf);

    /**
	 * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void nbModelPredict(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

    /**
	 * 构建聚类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClusters 聚类数目
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void kmModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:int numClusters, 5:DKMLConf dkmlconf);

    /**
	 * 聚类模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void kmModelPredict(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

    /**
	 * 聚类模型构建
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClusters 聚类数目
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void gmModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:int numClusters, 5:DKMLConf dkmlconf);

    /**
	 * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void gmModelPredict(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

    /**
	 * 主要用来对数据降维,去噪
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param outputPath 结果保存路径
     * @param k 主成分数目
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void pcaModel(1:string masterUrl, 2:string inputPath, 3:string outputPath, 4:int k, 5:DKMLConf dkmlconf);

    /**
	 * 主要用于挖掘关联规则的频繁项集
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param outputPath 训练结果保存路径
     * @param minSupport 最小支持度，默认0.3，相当于占30%，超过此支持度的将被选出
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void fpGrowthModelBuild(1:string masterUrl, 2:string inputPath, 3:string outputPath, 4:double minSupport, 5:DKMLConf dkmlconf);

    /**
	 * 推荐模型构建
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param rank 特征数目，默认10，用户打分时所考虑的特征角度
     * @param numIterations 迭代次数，推荐10-20，默认10
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void alsModelBuild(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:int rank, 5:int numIterations, 6:DKMLConf dkmlconf);

    /**
	 * 给产品推荐用户
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 预测用数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void rmUsers(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

    /**
	 * 给用户推荐产品
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 预测用数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    void rmpProducts(1:string masterUrl, 2:string inputPath, 3:string modelPath, 4:string outputPath, 5:DKMLConf dkmlconf);

}