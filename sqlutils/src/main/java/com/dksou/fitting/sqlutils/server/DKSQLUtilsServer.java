package com.dksou.fitting.sqlutils.server;

import com.dksou.fitting.sqlutils.service.DKSQLEngine;
import com.dksou.fitting.sqlutils.service.serviceimpl.DKSQLEngineImpl;
import com.dksou.fitting.sqlutils.utils.PropUtil;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.Properties;

public class DKSQLUtilsServer {
    public static TMultiplexedProcessor multiplexedProcessor;
    public static DKSQLEngineImpl sqlEngineHandler;
    public static DKSQLEngine.Processor sqlEngineProcessor;


    public static void main(String[] args) {
        try {
            multiplexedProcessor = new TMultiplexedProcessor();

            sqlEngineHandler = new DKSQLEngineImpl();
            sqlEngineProcessor = new DKSQLEngine.Processor(sqlEngineHandler);
            multiplexedProcessor.registerProcessor("SQLEngineService", sqlEngineProcessor);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(multiplexedProcessor);
                }
            };
            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(TMultiplexedProcessor processor) {
        try {
            Properties Prop = PropUtil.loadProp(System.getProperty("user.dir") + "/conf/sqlutils.properties");
            //Properties Prop = PropUtil.loadProp("D:\\workspace\\fitting\\fitting-sqlutils\\src\\main\\resources\\sqlutils.properties");
            int port = Integer.valueOf(Prop.getProperty("sqlutils.port"));
            TServerTransport serverTransport = new TServerSocket(port);
            TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            System.out.println("Start server on port " + port + "...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
