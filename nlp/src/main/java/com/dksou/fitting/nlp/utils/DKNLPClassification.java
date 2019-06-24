package com.dksou.fitting.nlp.utils;


import com.dksou.fitting.nlp.hadoop.HDFSDataSet;
import com.hankcs.hanlp.corpus.io.IOUtil;
import com.hankcs.hanlp.utility.GlobalObjectPool;
import com.hankcs.nlp.classifiers.IClassifier;
import com.hankcs.nlp.classifiers.LinearSVMClassifier;
import com.hankcs.nlp.classifiers.NaiveBayesClassifier;
import com.hankcs.nlp.corpus.IDataSet;
import com.hankcs.nlp.corpus.MemoryDataSet;
import com.hankcs.nlp.models.AbstractModel;
import com.hankcs.nlp.models.LinearSVMModel;
import com.hankcs.nlp.models.NaiveBayesModel;

/**
 * 文本分类（相似性）处理
 *
 * @author hankcs
 */
public class DKNLPClassification
{
    private DKNLPClassification()
    {
    }

    /**
     * 训练分类模型
     *
     * @param corpusPath 语料库目录
     * @param modelPath  模型保存路径
     */
    public static void trainModel(String corpusPath, String modelPath)
    {
        IClassifier classifier = new LinearSVMClassifier();
        try
        {
            IDataSet dataSet = DKNLPBase.configuration == null ? new MemoryDataSet().load(corpusPath) :
                    new HDFSDataSet(DKNLPBase.configuration).load(corpusPath);
            classifier.train(dataSet);
            IOUtil.saveObjectTo(classifier.getModel(), modelPath);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 文本分类
     *
     * @param modelPath 模型保存目录
     * @param filePath  待分类文本保存目录
     * @return 分类信息
     */
    public static String classify(String modelPath, String filePath)
    {
        if (modelPath == null || filePath == null) return null;
        Object model = GlobalObjectPool.get(modelPath);
        if (model == null)
        {
            model = IOUtil.readObjectFrom(modelPath);
            GlobalObjectPool.put(modelPath, model);
        }
        if (model instanceof AbstractModel)
            return classify((AbstractModel) model, filePath);
        return null;
    }

    /**
     * 文本分类
     *
     * @param model    模型
     * @param filePath 待分类文本保存目录
     * @return 分类信息
     */
    public static String classify(AbstractModel model, String filePath)
    {
        String txt = IOUtil.readTxt(filePath);
        if (txt == null) return null;

        IClassifier classifier;
        if (model instanceof LinearSVMModel)
        {
            classifier = new LinearSVMClassifier((LinearSVMModel) model);
        }
        else if (model instanceof NaiveBayesModel)
        {
            classifier = new NaiveBayesClassifier((NaiveBayesModel) model);
        }
        else return null;

        return classifier.classify(txt);
    }
}