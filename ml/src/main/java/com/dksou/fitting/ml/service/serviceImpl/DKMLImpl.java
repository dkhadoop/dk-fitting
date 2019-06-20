package com.dksou.fitting.ml.service.serviceImpl;

import com.dksou.fitting.ml.service.DKML;
import com.dksou.fitting.ml.service.DKMLConf;
import com.dksou.fitting.ml.utils.PathUtils;
import com.dksou.fitting.ml.utils.PropUtils;
import com.dksou.fitting.ml.utils.RuntimeSUtils;
import com.dksou.fitting.ml.utils.SparkSubmitUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Map;

public class DKMLImpl implements DKML.Iface{
    static Logger log = Logger.getLogger(DKML.class);
    Map<String, String> prop = PropUtils.getProp("dkml.properties");
    /**
     * 推荐模型构建
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param rank 特征数目，默认10，用户打分时所考虑的特征角度
     * @param numIterations 迭代次数，推荐10-20，默认10
     */
    @Override
    public void alsModelBuild(String masterUrl, String inputPath, String modelPath, int rank, int numIterations, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.als.ALSModelBuild";
        String logName = "alsModelBuild" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,rank+"",numIterations+"",dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 给产品推荐用户
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 预测用数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     */
    @Override
    public void rmUsers(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.als.RMUsers";
        String logName = "rmUsers" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 给用户推荐产品
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 预测用数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void rmpProducts(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.als.RMProducts";
        String logName = "rmpProducts";
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 主要用于挖掘关联规则的频繁项集
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param outputPath 训练结果保存路径
     * @param minSupport 最小支持度，默认0.3，相当于占30%，超过此支持度的将被选出
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void fpGrowthModelBuild(String masterUrl, String inputPath, String outputPath, double minSupport, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.fpgrowth.FPGrowthModel";
        String logName = "fpGrowthModelBuild";
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,outputPath,minSupport+"",dkmlpath});
        RuntimeSUtils.executeShell(command,logName);

    }

    /**
     * 聚类模型构建
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClusters 聚类数目
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void gmModelBuild(String masterUrl, String inputPath, String modelPath, int numClusters, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.gaussian.GMModelBuild";
        String logName = "gmModelBuild";
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,numClusters+"",dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void gmModelPredict(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.gaussian.GMModelPredict";
        String logName = "gmModelPredict" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 构建聚类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClusters 聚类数目
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void kmModelBuild(String masterUrl, String inputPath, String modelPath, int numClusters, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.kmeans.KMModelBuild";
        String logName = "kmModelBuild";
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,numClusters+"",dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 聚类模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void kmModelPredict(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.kmeans.KMModelPredict";
        String logName = "kmModelPredict" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }



    /**
     * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClass 分类数目
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void lrModelBuild(String masterUrl, String inputPath, String modelPath, int numClass, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.lr.LRModelBuild";
        String logName = "lrModelBuild" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,numClass+"",dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 结果保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void lrModelPredict(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.lr.LRModelPredict";
        String logName = "lrModelPredict";
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }


    /**
     * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void nbModelBuild(String masterUrl, String inputPath, String modelPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.nb.NBModelBuild";
        String logName = "nbModelBuild";
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 预测结果保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void nbModelPredict(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.nb.NBModelPredict";
        String logName = "nbModelPredict" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 主要用来对数据降维,去噪
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param outputPath 结果保存路径
     * @param k 主成分数目
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void pcaModel(String masterUrl, String inputPath, String outputPath, int k, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.pca.PCAModel";
        String logName = "pcaModel" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,outputPath,k+"",dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param numClass 分类数目
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void rfClassModelBuild(String masterUrl, String inputPath, String modelPath, int numClass, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.randomforest.RFClassModelBuild";
        String logName = "rfClassModelBuild" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,numClass+"",dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 构建回归模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void rfRegresModelBuild(String masterUrl, String inputPath, String modelPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.randomforest.RFRegresModelBuild";
        String logName = "rfRegresModelBuild" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 结果保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void rfModelPredict(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.randomforest.RFModelPredict";
        String logName = "rfModelPredict" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 构建分类模型
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void svmModelBuild(String masterUrl, String inputPath, String modelPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.svm.SVMModelBuild";
        String logName = "svmModelBuild" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 模型预测
     * @param masterUrl local[2], 或spark://IP:PORT
     * @param inputPath 训练数据所在路径
     * @param modelPath 模型保存路径
     * @param outputPath 结果保存路径
     * @param dkmlConf 为参数优化,可以使用默认值0表示使用系统默认参数。
     */
    @Override
    public void svmModelPredict(String masterUrl, String inputPath, String modelPath, String outputPath, DKMLConf dkmlConf) throws TException {
        String className = "com.dksou.freerch.ml.service.serviceImpl.svm.SVMModelPredict";
        String logName = "svmModelPredict" ;
        String dkmlpath = PathUtils.processingPathSeparator(prop.get("dkml.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkmlConf,className,new String[]{inputPath,modelPath,outputPath,dkmlpath});
        RuntimeSUtils.executeShell(command,logName);
    }



}
