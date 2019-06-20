package com.dksou.fitting.datasource.server;

import com.dksou.fitting.datasource.service.*;
import com.dksou.fitting.datasource.service.serviceimpl.*;
import com.dksou.fitting.datasource.util.PropUtil;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.Properties;

public class DKDataSourceServer {
    public static TMultiplexedProcessor multiplexedProcessor;

    public static DKDBInputImpl dkdbInputHandler;
    public static DKDBInput.Processor dkdbInputProcessor;

    public static DKDBOutputImpl dkdbOutputHandler;
    public static DKDBOutput.Processor dkdbOutputProcessor;

    public static DKFileInputImpl dkFileInputHandler;
    public static DKFileInput.Processor dkFileInputProcessor;

    public static DKFileOutputImpl dkFileOutputHandler;
    public static DKFileOutput.Processor dkFileOutputProcessor;

    public static DKESHandleImpl dkEsHandleHandler;
    public static DKESHandle.Processor dkEsHandleProcessor;

    public static void main(String[] args) {
        try {
            multiplexedProcessor = new TMultiplexedProcessor();

            dkdbInputHandler = new DKDBInputImpl();
            dkdbInputProcessor = new DKDBInput.Processor(dkdbInputHandler);

            dkdbOutputHandler = new DKDBOutputImpl();
            dkdbOutputProcessor = new DKDBOutput.Processor(dkdbOutputHandler);

            dkFileInputHandler = new DKFileInputImpl();
            dkFileInputProcessor = new DKFileInput.Processor(dkFileInputHandler);

            dkFileOutputHandler = new DKFileOutputImpl();
            dkFileOutputProcessor = new DKFileOutput.Processor(dkFileOutputHandler);

            dkEsHandleHandler = new DKESHandleImpl();
            dkEsHandleProcessor = new DKESHandle.Processor(dkEsHandleHandler);

            multiplexedProcessor.registerProcessor("DKDBInputService", dkdbInputProcessor);
            multiplexedProcessor.registerProcessor("DKDBOutputService", dkdbOutputProcessor);
            multiplexedProcessor.registerProcessor("DKFileInputService", dkFileInputProcessor);
            multiplexedProcessor.registerProcessor("DKFileOutputService", dkFileOutputProcessor);
            multiplexedProcessor.registerProcessor("DKESHandleService", dkEsHandleProcessor);

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
            Properties Prop = PropUtil.loadProp(System.getProperty("user.dir") + "/conf/datasource.properties");
            //Properties Prop = PropUtil.loadProp("D:\\workspace\\FreeRCH\\freerch-datasource\\src\\main\\resources\\datasource.properties");
            int port = Integer.valueOf(Prop.getProperty("datasource.port"));
            TServerTransport serverTransport = new TServerSocket(port);
            TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            System.out.println("Start server on port " + port + "...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

