package com.dksou.fitting.nlp.utils;

import com.dksou.fitting.nlp.hadoop.HDFSDataSet;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.io.IOUtil;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.dictionary.py.Pinyin;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import com.hankcs.hanlp.utility.GlobalObjectPool;
import com.hankcs.hanlp.utility.TextUtility;
import com.hankcs.nlp.NewWordDiscover;
import com.hankcs.nlp.WordInfo;
import com.hankcs.nlp.classifiers.IClassifier;
import com.hankcs.nlp.classifiers.LinearSVMClassifier;
import com.hankcs.nlp.classifiers.NaiveBayesClassifier;
import com.hankcs.nlp.corpus.IDataSet;
import com.hankcs.nlp.corpus.MemoryDataSet;
import com.hankcs.nlp.models.AbstractModel;
import com.hankcs.nlp.models.LinearSVMModel;
import com.hankcs.nlp.models.NaiveBayesModel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;


public class Nlputil {


    /**
     * 为HanLP内部的全局词典准备的锁
     */
    private static final byte[] lockCustomDictionary = new byte[0];


    public static void main(String[] args) {
        System.out.println(HanLP.segment("你好，欢迎使用HanLP汉语处理包！"));

        Nlputil nlputil = new Nlputil();
        String s = nlputil.segment("商品和服务");
        System.out.println("s->" + s);

    }


    /**
     * 标准分词
     *
     * @param txt 要分词的语句
     * @return 分词列表
     */
    public static String segment(String txt){
        String segmentString = "";
        if (txt == null) {
            return String.valueOf(Collections.emptyList());
        }
        List<Term> segment = StandardTokenizer.segment(txt.toCharArray());
        for(Term s : segment){

            segmentString = s +","+ segmentString;
        }
        return segmentString;
    }


    /**
     * 关键词提取
     *
     * @param txt    要提取关键词的语句
     * @param keySum 要提取关键字的数量
     * @return 关键词列表
     */
    public static String extractKeyword(String txt, int keySum){
        String keyString = "";
        if (txt == null || keySum <= 0){
            return String.valueOf(Collections.emptyList());
        }
        List<String> keyList = HanLP.extractKeyword(txt, keySum);
        for(String s : keyList){

            keyString = s +","+  keyString;
        }
        return keyString;
    }


    /**
     * 短语提取
     *
     * @param txt   文本
     * @param phSum 需要多少个短语
     * @return 短语列表
     */
    public static String extractPhrase(String txt, int phSum) {
        String phraseString = "";
        if (txt == null || phSum <= 0){
            return String.valueOf(Collections.emptyList());
        }
        List<String> phraseList = HanLP.extractPhrase(txt, phSum);
        for(String s : phraseList){
            phraseString = s +","+ phraseString;
        }
        return phraseString;
    }

    /**
     * 自动摘要
     *
     * @param txt  要提取摘要的语句
     * @param sSum 摘要句子数量
     * @return 摘要句子列表
     */
    public static String extractSummary(String txt, int sSum){
        String summaryString = "";
        if (txt == null || sSum <= 0){
            return String.valueOf(Collections.emptyList());
        }
        List<String> summaryList = HanLP.extractSummary(txt, sSum);
        for(String s : summaryList){
            summaryString = s +","+ summaryString;
        }
        return summaryString;
    }


    /**
     * 拼音转换
     *
     * @param txt 要转换拼音的语句
     * @return 拼音列表
     */
    public static String convertToPinyinList(String txt){
        String pinyinString = "";
        if (txt == null){
            return String.valueOf(Collections.emptyList());
        }
        List<Pinyin> pinyinList = HanLP.convertToPinyinList(txt);
        for(Pinyin s : pinyinList){
            pinyinString = s +","+ pinyinString;
        }
        return pinyinString;
    }

    /**
     * 添加词库
     *
     * @param filePath 新的词库文件，每个词使用回车换行分隔(编码UTF-8)
     * @return 空—完成，其它—错误信息
     */
    public static String addCK(String filePath)
    {
        String addCKString = addCK(filePath, "UTF-8");
        return addCKString;
    }

    /**
     * 添加词库
     *
     * @param filePath 新的词库文件，每个词使用回车换行分隔
     * @param encoding 编码
     * @return 空—完成，其它—错误信息
     */
    public static String addCK(String filePath, String encoding)
    {
        if (filePath == null || encoding == null) return String.format("参数错误:addCK(%s, %s)", filePath, encoding);
        try
        {
            BufferedReader br = new BufferedReader(new InputStreamReader(IOUtil.newInputStream(filePath), encoding));
            String line;
            synchronized (lockCustomDictionary)
            {
                while ((line = br.readLine()) != null)
                {
                    CustomDictionary.insert(line);
                }
            }
            br.close();
        } catch (Exception e) {
            System.out.println(e);
            return TextUtility.exceptionToString(e);

        }

        return "添加成功";
    }

    /**
     * 提取词语
     *
     * @param text 大文本
     * @param size 需要提取词语的数量
     * @return 一个词语列表
     */
    public static String findWords(String text, int size){
        String wordsString = "";
        List<WordInfo> wordsList = findWords(text, size, false);
        for(WordInfo s : wordsList){
            wordsString = s +","+ wordsString;
        }
        return wordsString;
    }

    /**
     * 提取词语
     *
     * @param text         大文本
     * @param size         需要提取词语的数量
     * @param newWordsOnly 是否只提取词典中没有的词语
     * @return 一个词语列表
     */
    public static List<WordInfo> findWords(String text, int size, boolean newWordsOnly)
    {
        NewWordDiscover discover = new NewWordDiscover(4, 0.0f, .5f, 100f, newWordsOnly);
        return discover.discovery(text, size);
    }


    /**
     * 训练分类模型
     *  @param corpusPath 语料库目录
     * @param modelPath  模型保存路径
     */
    public static String trainModel(String corpusPath, String modelPath)
    {
        IClassifier classifier = new LinearSVMClassifier();
        try
        {
            IDataSet dataSet = DKNLPBase.configuration == null ? new MemoryDataSet().load(corpusPath) : new HDFSDataSet(DKNLPBase.configuration).load(corpusPath);
            classifier.train(dataSet);
            IOUtil.saveObjectTo(classifier.getModel(), modelPath);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return e.toString();
        }

        return "完成";
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
        if (modelPath == null || filePath == null){
            return "路径为空";
        }
        Object model = GlobalObjectPool.get(modelPath);
        if (model == null){
            model = IOUtil.readObjectFrom(modelPath);
            GlobalObjectPool.put(modelPath, model);
        }
        if (model instanceof AbstractModel){
            return classify((AbstractModel) model, filePath);
        }
        return "成功";
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
