package com.dksou.fitting.graphx.service;
import com.dksou.fitting.graphx.service.serviceImpl.DKGraphxImpl;
import com.dksou.fitting.graphx.utils.PropUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.Map;


public class DKGraphxServer {
    static Map<String, String> prop = PropUtils.getProp("dkgraphx.properties");

    /**
     * 阻塞式、多线程处理
     *
     * @param args
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) {
        try {
            int port = Integer.valueOf(prop.get("dkgraphx.port"));
            //设置传输通道，普通通道
            TServerTransport serverTransport = new TServerSocket(port);

            //使用高密度二进制协议
            TProtocolFactory proFactory = new TCompactProtocol.Factory();

            //设置处理器
            TProcessor processor = new DKGraphx.Processor(new DKGraphxImpl());

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
