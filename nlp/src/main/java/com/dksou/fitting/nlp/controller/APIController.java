package com.dksou.fitting.nlp.controller;



import com.dksou.fitting.nlp.utils.Nlputil;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class APIController {

    /**
     * 标准分词
     * @param txt
     * @return
     */
    @RequestMapping("/segment")
    public String segment(String txt){
        String segment = Nlputil.segment(txt);
        return segment;

    }

    /**
     * 关键词提取
     * @param txt
     * @param keySum
     * @return
     */
    @RequestMapping("/extractKeyword")
    public String extractKeyword(String txt,int keySum){
        String keyString = Nlputil.extractKeyword(txt, keySum);
        return keyString;
    }
    
    /**
     * 短语提取
     * @param txt
     * @param phSum
     * @return
     */
    @RequestMapping("/extractPhrase")
    public String extractPhrase(String txt, int phSum){
        String phraseString = Nlputil.extractPhrase(txt, phSum);
        return  phraseString;
    }


    /**
     * 自动摘要
     * @param txt
     * @param sSum
     * @return
     */
    @RequestMapping("/extractSummary")
    public String extractSummary(String txt,int sSum){
        String sentenceList = Nlputil.extractPhrase(txt,sSum);
        return sentenceList;
    }

    /**
     * 拼音转换
     * @param txt
     * @return
     */
    @RequestMapping("/convertToPinyinList")
    public String convertToPinyinList(String txt){
        String pinyinList = Nlputil.convertToPinyinList(txt);
        return  pinyinList;
    }


    /**
     * 添加词库
     * @param filePath
     */
    @RequestMapping(value="/addCK")
    public String  addCK(String filePath){
        String addCKString = Nlputil.addCK(filePath);
        return  addCKString;
    }


    /**
     * 提取词语
     *
     * @param text 大文本
     * @param size 需要提取词语的数量
     * @return 一个词语列表
     */
    @RequestMapping("/findWords")
    public static String findWords(String text,int size,boolean b){
        String wordsString = Nlputil.findWords(text, size);
        return  wordsString;
    }


    /**
     * 训练分类模型
     *
     * @param corpusPath 语料库目录
     * @param modelPath  模型保存路径
     */
    @RequestMapping("/trainModel")
    public static String trainModel(String corpusPath,String modelPath){
        String s = Nlputil.trainModel(corpusPath, modelPath);
        return  s;
    }


    /**
     * 文本分类
     *
     * @param modelPath 模型保存目录
     * @param filePath  待分类文本保存目录
     * @return 分类信息
     */
    @RequestMapping("/classify")
    public static String classify(String modelPath,String filePath){
        String classify = Nlputil.classify(modelPath, filePath);
        return classify;
    }




}
