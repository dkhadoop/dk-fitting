package com.dksou.fitting.server;

import com.dksou.fitting.search.SearchIOService.DKSearchInput;
import com.dksou.fitting.search.SearchIOService.DKSearchOutput;
import com.dksou.fitting.search.SearchIOService.impl.DKSearchInputImpl;
import com.dksou.fitting.search.SearchIOService.impl.DKSearchOutputImpl;
import com.dksou.fitting.search.SearchService.SearchService;
import com.dksou.fitting.search.SearchService.impl.SearchServiceImpl;
import com.dksou.fitting.utils.PropUtils;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

public  class DKSearchService{
    //public static final int SERVER_PORT=9000;
    static Map<String, String> prop = PropUtils.getProp("dkSearch.properties");
    /**
     *
     * 简单的单线程模式
     *
     */
    public  void  startService(){
        System.out.println( " start ...." );
        //单接口声明
        //TProcessor tProcessor=new DKSearchOutput.Processor<DKSearchOutput.Iface>( new DKSearchOutputImpl() );
        //多接口声明
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor( "DKSearchOutput",new DKSearchOutput.Processor<DKSearchOutput.Iface>( new DKSearchOutputImpl() ) );
        processor.registerProcessor( "DKSearchInput",new DKSearchInput.Processor<DKSearchInput.Iface>(new DKSearchInputImpl()  ) );
        processor.registerProcessor( "DKSearchService",new SearchService.Processor<SearchService.Iface>(new SearchServiceImpl() ) );
        try {
            TServerSocket serverSocket=new TServerSocket( Integer.valueOf(prop.get("dkSearch.port")) );
            TServer.Args targs=new TServer.Args( serverSocket );
            targs.processor( processor );
            //二进制TBinaryProtocol
            //二进制高密度TCompactProtocol
            targs.protocolFactory( new TCompactProtocol.Factory(  ) );
            TServer tServer=new TSimpleServer( targs );
            tServer.serve();
        } catch (TTransportException e) {
            System.err.println("Service Start error");
            e.printStackTrace();
        }
    }
    /**
     *  线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求。
     */
    public void startTThreadPoolServer() {
        try {
            System.out.println("UserInfoServiceDemo TThreadPoolServer start ....");
            TMultiplexedProcessor processor = new TMultiplexedProcessor();
            processor.registerProcessor( "DKSearchOutput",new DKSearchOutput.Processor<DKSearchOutput.Iface>( new DKSearchOutputImpl() ) );
            processor.registerProcessor( "DKSearchInput",new DKSearchInput.Processor<DKSearchInput.Iface>(new DKSearchInputImpl()  ) );
            processor.registerProcessor( "DKSearchService",new SearchService.Processor<SearchService.Iface>(new SearchServiceImpl() ) );
            //TProcessor tprocessor = new UserInfoService.Processor<UserInfoService.Iface>(new UserInfoServiceImpl());
            TServerSocket serverTransport = new TServerSocket( Integer.valueOf(prop.get("dkSearch.port")));
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
        DKSearchService dks=new DKSearchService();
        //dks.startService();
        dks.startTThreadPoolServer();


    }
}
