package com.dksou.fitting.graphx.utils;

import com.dksou.fitting.graphx.service.DKGraphxConf;
import org.apache.log4j.Logger;

import java.util.Map;

public class SparkSubmitUtils {
    static Logger log = Logger.getLogger(SparkSubmitUtils.class);
    /**
     *
     * @param prop   属性文件名称，得到环境变量。
     * @param dkmlConf  sparksubmit 参数
     * @param className 要提交的类名
     * @return   返回组拼好的命令。
     */
    public static String parameterAssembly(String masterUrl, Map<String,String> prop, DKGraphxConf dkmlConf, String className, String[] args) {
        StringBuffer command = new StringBuffer();

        String spark_bin_home = PathUtils.processingPathSeparator(prop.get("spark_bin_home"));

        command.append(spark_bin_home + "spark-submit " + " ");
        command.append("--master" + " " + masterUrl + " ");

        String driver_memory = dkmlConf.DRIVER_MEMORY.toLowerCase();
        String executor_cores = dkmlConf.EXECUTOR_CORES.toLowerCase();
        String executor_memory = dkmlConf.EXECUTOR_MEMORY.toLowerCase();
        String num_executors = dkmlConf.NUM_EXECUTORS.toLowerCase();
        String queue = dkmlConf.QUEUE;
        String principal = dkmlConf.PRINCIPAL;
        String keytab = dkmlConf.KEYTAB;

        if(!"".equals(driver_memory) && driver_memory != null && !"0g".equals(driver_memory)){
            command.append("--driver-memory" + " " + driver_memory + " ");
        }
        if(!"".equals(executor_cores) && executor_cores != null && !"0".equals(executor_cores)){
            command.append("--executor-cores" + " " + executor_cores + " ");
        }
        if(!"".equals(executor_memory) && executor_memory != null && !"0g".equals(executor_memory)){
            command.append("--executor-memory" + " " + executor_memory + " ");
        }
        if(!"".equals(num_executors) && num_executors != null && !"0".equals(num_executors)){
            command.append("--num-executors" + " " + num_executors + " ");
        }
        if(!"".equals(queue) && queue != null && !"default".equals(queue)){
            command.append("--queue" + " " + queue + " ");
        }
        if(!"".equals(principal) && principal != null && !"default".equals(principal)){
            command.append("--principal" + " " + principal + " ");
        }
        if(!"".equals(keytab) && keytab != null && !"default".equals(keytab)){
            command.append("--keytab" + " " + keytab + " ");
        }


        command.append("--class" + " " + className + " ");
        String dkgraphxpath = PathUtils.processingPathSeparator(prop.get("dkgraphx.path"));
        command.append(dkgraphxpath + "lib/graphx-1.0.jar" + " ");
// freerch-graphx-2.0.jar
        for(String arg : args){
            command.append(arg + " ");
        }

        command.append(">>" + " " + dkgraphxpath + "logs/dkgraphx.log"  + " ");


        log.warn(command.toString());
        return command.toString();
    }

}
