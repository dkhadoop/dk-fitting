package com.dksou.fitting.ml.service;
import com.dksou.fitting.ml.service.serviceImpl.DKMLImpl;
import com.dksou.fitting.ml.utils.PropUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.Map;

public class DKMLServer {
    static Map<String, String> prop = PropUtils.getProp("dkml.properties");

    /**
     * 阻塞式、多线程处理
     *
     * @param args
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) {
        try {
            int port = Integer.valueOf(prop.get("dkml.port"));
            //设置传输通道，普通通道
            TServerTransport serverTransport = new TServerSocket(port);
            //使用高密度二进制协议
            TProtocolFactory proFactory = new TCompactProtocol.Factory();
            //设置处理器
            TProcessor processor = new DKML.Processor(new DKMLImpl());
            //创建服务器
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport)
                            .protocolFactory(proFactory)
                            .processor(processor)
            );

            System.out.println("Start server on port "+ port +"...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
