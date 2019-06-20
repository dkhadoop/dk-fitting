package com.dksou.fitting.graphx.service.serviceImpl;

import com.dksou.fitting.graphx.service.DKGraphx;
import com.dksou.fitting.graphx.service.DKGraphxConf;
import com.dksou.fitting.graphx.utils.PathUtils;
import com.dksou.fitting.graphx.utils.PropUtils;
import com.dksou.fitting.graphx.utils.SparkSubmitUtils;
import com.dksou.fitting.graphx.utils.dklouvain.RuntimeSUtils;
import org.apache.thrift.TException;

import java.util.Map;

public class DKGraphxImpl implements DKGraphx.Iface{

    Map<String, String> prop = PropUtils.getProp("dkgraphx.properties");

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
    @Override
    public void pageRank(String masterUrl, String inputPath, String delimiter, String dataType, String outputPath, DKGraphxConf dkgraphxconf) throws TException {
        String className = "com.dksou.freerch.graphx.service.serviceImpl.DKPageRank";
        String logName = "pageRank" ;
        String dkgraphxpath = PathUtils.processingPathSeparator(prop.get("dkgraphx.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkgraphxconf,className,new String[]{inputPath,delimiter,dataType,outputPath,dkgraphxpath});
        RuntimeSUtils.executeShell(command,logName);
    }

    /**
     * 主要用于社区中的人群分类，可根据人群的类别进行广告推送等。
     * @param masterUrl local[*], 或spark://IP:PORT
     * @param inputPath 数据所在路径
     * @param delimiter 分隔符
     * @param outputPath 结果输出目录
     * @param dkgraphxconf 为参数优化,可以使用默认值0表示使用系统默认参数。
     * @throws TException
     */
    @Override
    public void commDetect(String masterUrl, String inputPath, String delimiter, String outputPath, DKGraphxConf dkgraphxconf) throws TException {
        String className = "com.dksou.freerch.graphx.service.serviceImpl.DKlouvain";
        String logName = "commDetect" ;
        String dkgraphxpath = PathUtils.processingPathSeparator(prop.get("dkgraphx.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkgraphxconf,className,new String[]{inputPath,delimiter,outputPath,dkgraphxpath});
        RuntimeSUtils.executeShell(command,logName);
    }

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
    @Override
    public void frindsFind(String masterUrl, String inputPath, String delimiter, String outputPath, DKGraphxConf dkgraphxconf) throws TException {
        String className = "com.dksou.freerch.graphx.service.serviceImpl.DKShortPaths";
        String logName = "frindsFind" ;
        String dkgraphxpath = PathUtils.processingPathSeparator(prop.get("dkgraphx.path")) + "lib/";
        String command = SparkSubmitUtils.parameterAssembly(masterUrl,prop,dkgraphxconf,className,new String[]{inputPath,delimiter,outputPath,dkgraphxpath});
        RuntimeSUtils.executeShell(command,logName);
    }
}
