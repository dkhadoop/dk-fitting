package com.dksou.fitting.dataprocess.server;

import com.dksou.fitting.dataprocess.service.*;
import com.dksou.fitting.dataprocess.service.impl.*;
import com.dksou.fitting.dataprocess.util.PropUtils;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import java.util.Map;
import java.util.Properties;


/**
 * 启动主方法
 */
public class DataProcess {

    Properties providerProp = PropUtils.getProp("dataprocess.properties");
    int SERVER_PORT = Integer.parseInt(providerProp.getProperty("dataprocess.port"));



    public void startServer() {

        try {
            System.out.println("Start server on port "+ SERVER_PORT +"...");

            // 简单的单线程服务模型，一般用于测试
            TServerSocket serverTransport = new TServerSocket(SERVER_PORT);

            TMultiplexedProcessor processor = new TMultiplexedProcessor();


            DataAnalysisService.Processor<DataAnalysisService.Iface> tprocessor =
                    new DataAnalysisService.Processor<>(new DataAnalysisImpl());
            processor.registerProcessor("DataAnalysisService", tprocessor);


            DataStaticKerberosService.Processor<DataStaticKerberosService.Iface> tprocessor1 =
                    new DataStaticKerberosService.Processor<DataStaticKerberosService.Iface>(new DataStaticKerberosImpl());
            processor.registerProcessor("DataStaticKerberosService", tprocessor1);

            DataStaticService.Processor<DataStaticService.Iface> tprocessor2 =
                    new DataStaticService.Processor<DataStaticService.Iface>(new DataStaticImpl());
            processor.registerProcessor("DataStaticService", tprocessor2);


            DedupeKerberosService.Processor<DedupeKerberosService.Iface> tprocessor3 =
                    new DedupeKerberosService.Processor<DedupeKerberosService.Iface>(new DedupeKerberosImpl());
            processor.registerProcessor("DedupeKerberosService", tprocessor3);


            DedupeService.Processor<DedupeService.Iface> tprocessor4 =
                    new DedupeService.Processor<DedupeService.Iface>(new DedupeImpl());
            processor.registerProcessor("DedupeService", tprocessor4);

            FormatFieldService.Processor<FormatFieldService.Iface> tprocessor5 =
                    new FormatFieldService.Processor<FormatFieldService.Iface>(new FormatFieldImpl());
            processor.registerProcessor("FormatFieldService", tprocessor5);


            FormatRecService.Processor<FormatRecService.Iface> tprocessor6 =
                    new FormatRecService.Processor<FormatRecService.Iface>(new FormatRecImpl());
            processor.registerProcessor("FormatRecService", tprocessor6);

            SelectFieldService.Processor<SelectFieldService.Iface> tprocessor7 =
                    new SelectFieldService.Processor<SelectFieldService.Iface>(new SelectFieldImlp());
            processor.registerProcessor("SelectFieldService", tprocessor7);

            SelectRecService.Processor<SelectRecService.Iface> tprocessor8 =
                    new SelectRecService.Processor<SelectRecService.Iface>(new SelectRecImpl());
            processor.registerProcessor("SelectRecService", tprocessor8);


            DataCleanKerberosService.Processor<DataCleanKerberosService.Iface> tprocessor9 =
                    new DataCleanKerberosService.Processor<DataCleanKerberosService.Iface>(new DataCleanKerberosImpl());
            processor.registerProcessor("DataCleanKerberosService", tprocessor9);

            TThreadPoolServer.Args ttpsArgs = new TThreadPoolServer.Args(serverTransport);
            ttpsArgs.processor(processor);
            ttpsArgs.protocolFactory(new TBinaryProtocol.Factory());

            // 线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求。
            TServer server = new TThreadPoolServer(ttpsArgs);
            server.serve();



        } catch (Exception e) {
            System.out.println("Server start error!!!");
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {

        DataProcess dataProcess = new DataProcess();
        dataProcess.startServer();


    }



}
