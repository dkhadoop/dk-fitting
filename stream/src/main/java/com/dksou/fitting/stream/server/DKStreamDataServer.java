package com.dksou.fitting.stream.server;

import com.dksou.fitting.stream.service.DKStreamProducerService;
import com.dksou.fitting.stream.service.JavaKafkaConsumerHighAPIESService;
import com.dksou.fitting.stream.service.JavaKafkaConsumerHighAPIHdfsService;
import com.dksou.fitting.stream.service.impl.DKStreamProducerServiceImpl;
import com.dksou.fitting.stream.service.impl.JavaKafkaConsumerHighAPIESImpl;
import com.dksou.fitting.stream.service.impl.JavaKafkaConsumerHighAPIHdfsImpl;
import com.dksou.fitting.stream.utils.PropUtils;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import java.util.Properties;

public class DKStreamDataServer {




    Properties providerProp = PropUtils.getProp("stream.properties");
    int SERVER_PORT = Integer.parseInt(providerProp.getProperty("server.port"));


    public void startServer() {

        try {
            System.out.println("Server start ....");


            // 简单的单线程服务模型，一般用于测试
            TServerSocket serverTransport = new TServerSocket(  SERVER_PORT  );
            TMultiplexedProcessor processor = new TMultiplexedProcessor();



            //注册一个服务接口
            DKStreamProducerService.Processor<DKStreamProducerServiceImpl> pro =
                    new DKStreamProducerService.Processor<DKStreamProducerServiceImpl>(new DKStreamProducerServiceImpl());
            processor.registerProcessor("DKStreamProducerService", pro);

            //注册一个服务接口
            JavaKafkaConsumerHighAPIESService.Processor<JavaKafkaConsumerHighAPIESImpl> pro1 =
                    new JavaKafkaConsumerHighAPIESService.Processor<JavaKafkaConsumerHighAPIESImpl>(new JavaKafkaConsumerHighAPIESImpl());
            processor.registerProcessor("ESService", pro1);



            //注册一个服务接口

            JavaKafkaConsumerHighAPIHdfsService.Processor<JavaKafkaConsumerHighAPIHdfsImpl> pro3 =
                    new JavaKafkaConsumerHighAPIHdfsService.Processor<>(new JavaKafkaConsumerHighAPIHdfsImpl());
            processor.registerProcessor("JavaKafkaConsumerHighAPIHdfsService", pro3);


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

        DKStreamDataServer dataProcess = new DKStreamDataServer();
        dataProcess.startServer();


    }




}
