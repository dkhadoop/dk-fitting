package com.dksou.fitting.nlp.controller;


import com.dksou.fitting.nlp.utils.DKNLPBase;
import com.dksou.fitting.nlp.utils.DKNLPClassification;
import com.hankcs.hanlp.dictionary.py.Pinyin;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.nlp.WordInfo;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class NLPController {


    /**
     * 标准分词
     * @param txt
     * @return
     */
    @RequestMapping(value="/segment", method= RequestMethod.POST)
    public static List<Term> segment(@RequestBody String txt){
        List<Term> segment = DKNLPBase.segment(txt);
        return segment;
    }

    /**
     * 关键词提取
     * @param txt
     * @param keySum
     * @return
     */
    @RequestMapping(value="/extractKeyword", method= RequestMethod.POST)
    public static List<String> extractKeyword(@RequestBody String txt,@RequestBody int keySum){
        List<String> sentenceList = DKNLPBase.extractKeyword(txt, keySum);
        return sentenceList;

    }

    /**
     * 短语提取
     * @param txt
     * @param phSum
     * @return
     */
    @RequestMapping(value="/extractPhrase", method= RequestMethod.POST)
    public static List<String> extractPhrase(@RequestBody String txt,@RequestBody int phSum){
        List<String> phraseList = DKNLPBase.extractPhrase(txt, phSum);
        return  phraseList;
    }

    /**
     * 自动摘要
     * @param txt
     * @param sSum
     * @return
     */
    @RequestMapping(value="/extractSummary", method= RequestMethod.POST)
    public static List<String> extractSummary(@RequestBody String txt,@RequestBody int sSum){
        List<String> sentenceList = DKNLPBase.extractPhrase(txt, sSum);
        return sentenceList;
    }

    @RequestMapping(value="/convertToPinyinList", method= RequestMethod.POST)
    public static List<Pinyin> convertToPinyinList(@RequestBody String txt){
        List<Pinyin> pinyinList = DKNLPBase.convertToPinyinList(txt);
        return  pinyinList;
    }

    /**
     * 添加词库
     * @param filePath
     */
    @RequestMapping(value="/addCK", method= RequestMethod.POST)
    public static void addCK(@RequestBody String filePath){
        DKNLPBase.addCK(filePath);
    }


    @RequestMapping(value="/findWords", method= RequestMethod.POST)
    public static List<WordInfo> findWords(@RequestBody String text, @RequestBody int size, @RequestBody boolean b){
        List<WordInfo> words = DKNLPBase.findWords(text, size);
        return  words;
    }


    @RequestMapping(value="/trainModel", method= RequestMethod.POST)
    public static void trainModel(@RequestBody String corpusPath,@RequestBody String modelPath){
        DKNLPClassification.trainModel(corpusPath,modelPath);
    }


    @RequestMapping(value="/classify", method= RequestMethod.POST)
    public static String classify(@RequestBody String modelPath,@RequestBody String filePath){
        String classify = DKNLPClassification.classify(modelPath, modelPath);
        return classify;
    }







}
