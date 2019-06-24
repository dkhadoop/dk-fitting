package com.dksou.fitting.nlp.utils;


import com.dksou.fitting.nlp.hadoop.HDFSIOAdapter;
import com.hankcs.cluster.ClusterAnalyzer;
import com.hankcs.cluster.ITokenizer;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.io.IOUtil;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.dictionary.py.Pinyin;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import com.hankcs.hanlp.utility.TextUtility;
import com.hankcs.nlp.NewWordDiscover;
import com.hankcs.nlp.WordInfo;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * DKNLPBase 工具类
 *
 * @author hankcs
 */
public class DKNLPBase
{
    /**
     * 为HanLP内部的全局词典准备的锁
     */
    private static final byte[] lockCustomDictionary = new byte[0];
    /**
     * HDFS配置项
     */
    protected static Configuration configuration;

    private DKNLPBase()
    {
    }

    /**
     * 启用Hadoop模式（所有数据读写全部重定向到HDFS）
     *
     * @param configuration Hadoop的配置
     * @throws IllegalArgumentException 由于配置不正确产生的异常
     */
    public static void enableHadoop(Configuration configuration) throws IllegalArgumentException
    {
        HanLP.Config.IOAdapter = new HDFSIOAdapter(configuration);
        DKNLPBase.configuration = configuration;
    }

    /**
     * 标准分词
     *
     * @param txt 要分词的语句
     * @return 分词列表
     */
    public static List<Term> segment(String txt)
    {
        if (txt == null) return Collections.emptyList();
        return StandardTokenizer.segment(txt.toCharArray());
    }

    /**
     * 关键词提取
     *
     * @param txt    要提取关键词的语句
     * @param keySum 要提取关键字的数量
     * @return 关键词列表
     */
    public static List<String> extractKeyword(String txt, int keySum)
    {
        if (txt == null || keySum <= 0) return Collections.emptyList();
        return HanLP.extractKeyword(txt, keySum);
    }

    /**
     * 短语提取
     *
     * @param txt   文本
     * @param phSum 需要多少个短语
     * @return 短语列表
     */
    public static List<String> extractPhrase(String txt, int phSum)
    {
        if (txt == null || phSum <= 0) return Collections.emptyList();
        return HanLP.extractPhrase(txt, phSum);
    }

    /**
     * 自动摘要
     *
     * @param txt  要提取摘要的语句
     * @param sSum 摘要句子数量
     * @return 摘要句子列表
     */
    public static List<String> extractSummary(String txt, int sSum)
    {
        if (txt == null || sSum <= 0) return Collections.emptyList();
        return HanLP.extractSummary(txt, sSum);
    }

    /**
     * 拼音转换
     *
     * @param txt 要转换拼音的语句
     * @return 拼音列表
     */
    public static List<Pinyin> convertToPinyinList(String txt)
    {
        if (txt == null) return Collections.emptyList();

        return HanLP.convertToPinyinList(txt);
    }

    /**
     * 添加词库
     *
     * @param filePath 新的词库文件，每个词使用回车换行分隔(编码UTF-8)
     * @return 空—完成，其它—错误信息
     */
    public static String addCK(String filePath)
    {
        return addCK(filePath, "UTF-8");
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
        }
        catch (Exception e)
        {
            return TextUtility.exceptionToString(e);
        }

        return null;
    }

    /**
     * 提取词语
     *
     * @param text 大文本
     * @param size 需要提取词语的数量
     * @return 一个词语列表
     */
    public static List<WordInfo> findWords(String text, int size)
    {
        return findWords(text, size, false);
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
     * 聚类
     *
     * @param documents 待聚类的文档集合，键为文档id，值为文档内容
     * @param size      需要得到的类别数量
     * @return 类目表, 每个类目内部是一个[文档id]=[相似程度]的列表
     */
    public static List<List<Map.Entry<String, Double>>> cluster(Map<String, String> documents, int size)
    {
        ClusterAnalyzer analyzer = new ClusterAnalyzer();
        analyzer.setTokenizer(new ITokenizer()
        {
            public String[] segment(final String text)
            {
                List<Term> termList = DKNLPBase.segment(text);
                ListIterator<Term> listIterator = termList.listIterator();
                while (listIterator.hasNext())
                {
                    if (CoreStopWordDictionary.shouldRemove(listIterator.next()))
                    {
                        listIterator.remove();
                    }
                }
                String[] termArray = new String[termList.size()];
                int i = 0;
                for (Term term : termList)
                {
                    termArray[i] = term.word;
                    ++i;
                }
                return termArray;
            }
        });
        for (Map.Entry<String, String> entry : documents.entrySet())
        {
            analyzer.addDocument(entry.getKey(), entry.getValue());
        }
        return analyzer.clusters(size);
    }
}