package com.dksou.fitting.search.SearchIOService.impl;

import com.dksou.fitting.search.SearchIOService.DKSearchInput;
import com.dksou.fitting.search.SearchIOService.FileData;
import com.dksou.fitting.utils.ESUtils;
import com.dksou.fitting.utils.FileEncode;
import com.dksou.fitting.utils.MD5Utils;
import com.dksou.fitting.utils.OfficeUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.thrift.TException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class DKSearchInputImpl implements DKSearchInput.Iface {
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public  static Client client;

    /**
     *
     * @param fs hdfs的uri路径：hdfs://192.168.43.101:8020
     * @param dirName hdfs的文件路径
     * @param hostIps ES的集群IP
     * @param clusterName ES的集群名称
     * @param indexName ES的索引名称
     * @param typeName ES的类型名称
     * @param port ES的端口号
     * @param length 每次导入ES数据的长度
     * @return
     * @throws TException
     * @throws IOException
     */
    @Override
    public String nosql2ES(String fs, String dirName, String hostIps, String clusterName, String indexName, String typeName, int port, int length) throws TException, IOException {
        ESUtils.createIndex( hostIps,port,clusterName,indexName,typeName );
        FSDataInputStream open1=null;
        FSDataInputStream open=null;
        BufferedReader br = null;
        try {
            String hdfsPath=fs+dirName;
        Configuration conf=new Configuration( true );
        FileSystem  fileSystem=FileSystem.get( URI.create( hdfsPath ),conf );
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles( new Path( hdfsPath ),true );
        while (listFiles.hasNext()){
            LocatedFileStatus next=listFiles.next();
            final Path path=next.getPath();
            if (fileSystem.isFile( path )){
                XContentBuilder b;
                try {
                 open = fileSystem.open( path, 2048 );
                 open1 = fileSystem.open( path, 2048 );
                } catch (IOException e) {
                    e.printStackTrace();
                }
                int available = open.available();
                String fileEncode = FileEncode.getFileEncode( open1 );
                //根据编码格式解析
                if ("asci".equals( fileEncode )){
                    // 这里采用GBK编码，而不用环境编码格式，因为环境默认编码不等于操作系统编码
                    // code = System.getProperty("file.encoding");
                    fileEncode="GBK";
                }
                byte[] bytes = new byte[0];
                    bytes = inputStream2Bytes( open, fileEncode );
                try {
                if (length*1024<available){
                    String text=new String(new String( bytes,0,length * 1024 ).getBytes(),"utf-8");
                        br=readLineAndInsertEs( hostIps,indexName,typeName,clusterName,port,text );
                }else {
                    String text=new String(  bytes,"utf-8");
                    br=readLineAndInsertEs( hostIps,indexName,typeName,clusterName,port,text );
                }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e.getMessage());
                }
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            try{
            if (open!=null){
                open.close();
            }
            if (open1!=null){
                open1.close();
            }
            if (br!=null){
                br.close();
            }
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        }
        return "SUCCESS";
    }

    /**
     *
     * @param fileType 文件类型，1=txt，2=doc，docx，3=xls，xlsx，4=pdf
     * @param filePath 文件路径
     * @param hostIps ES集群的IP地址
     * @param clusterName ES集群名称
     * @param indexName 索引名称
     * @param typeName ES类型
     * @param port ES的端口号
     * @param length ES一次导入数据的长度
     * @return
     * @throws TException
     */
    @Override
    public String file2ES(int fileType, String filePath, String hostIps, String clusterName, String indexName, String typeName, int port, int length) throws Exception {
        try {
            client=ESUtils.getClient( hostIps,port,clusterName );
        } catch (Exception e) {
            e.printStackTrace();
            return "连接集群失败";
        }
        IndicesExistsResponse existsResponse = client.admin().indices().prepareExists( indexName ).execute().actionGet();
        if (!existsResponse.isExists()){
            ESUtils.createIndex( hostIps, port, clusterName, indexName, typeName );
        }
        String[] type=null;
        if (fileType==1){
            type=new String[]{"txt"};
        }else if (fileType==2){
            type=new String[]{"doc","docx"};
        }else if (fileType==3){
            type=new String[]{"xls","xlsx"};
        }else if (fileType==4){
            type=new String[]{"pdf"};
        }else {
            return "Error filetype:"+fileType;
        }
        //File file=new File( filePath );
        //files( type, hostIps, clusterName, indexName, typeName, port, length, file );
        Collection<File>  files = FileUtils.listFiles( new File( filePath ), null, true );

        for (final File file:files){
            //加入索引
            XContentBuilder b;
            String md5;
            try {
                b=jsonBuilder().startObject();
                String fileName = file.getName();
                b.field( "v1",fileName );
                String absolutePath = file.getAbsolutePath();
                b.field( "v2",absolutePath );
                long lastModified = file.lastModified();
                String format = df.format( new Date( lastModified ) );
                b.field( "v3",format );
                String key=absolutePath+","+fileName+","+format;
                md5 = MD5Utils.md5( key );
                byte[] bytes = null;
                if (fileType==2){
                    String docOrDocx = OfficeUtils.ParseDocOrDocx( absolutePath );
                    bytes = docOrDocx.getBytes( "utf-8" );
                }else if (fileType==3){
                    String poiExcel2Txt = OfficeUtils.poiExcel2Txt( absolutePath );
                    bytes = poiExcel2Txt.getBytes( "utf-8" );
                }else if (fileType==4){
                    String itextPdf2Txt = OfficeUtils.itextPdf2Txt( absolutePath );
                    bytes=itextPdf2Txt.getBytes("utf-8");
                }else {
                    String fileEncode = FileEncode.getFileEncode( absolutePath );
                    if ("asci".equals( fileEncode )){
                        fileEncode="GBK";
                    }
                    bytes=FileUtils.readFileToString( file,fileEncode ).getBytes("utf-8");
                }
                if (length * 1024 <bytes.length){
                    b.field( "v4" ,new String( new String( bytes,"utf-8").getBytes(),0,bytes.length,"utf-8" ) );
                    b.field( "analyzer","hanlp-index" );
                    b.field("search_analyzer", "hanlp-index");
                }else {
                    b.field( "v4",new String( bytes,"utf-8" ) );
                    b.field( "analyzer","hanlp-index" );
                    b.field("search_analyzer", "hanlp-index");
                    //System.out.println("new String(bytes) = " + new String(bytes,"utf-8"));
                }
                b.endObject();

                ESUtils.insert( hostIps,port,clusterName,indexName,typeName,md5,b);

            } catch (Exception e) {
                e.printStackTrace();
                client.close();
               // throw new RuntimeException(e.getMessage());
            }
        }
        return "SUCCESS";

    }


    /**
     *
     * @param fileName 文件名字
     * @param fileDir  文件所在目录
     * @param hostIps  ES集群的IP
     * @param clusterName ES集群的名称
     * @param indexName  ES的索引名称
     * @param typeName ES的类型名称
     * @param port 端口号
     * @param fileData 文件数据 包括（文件名称，二进制数据）
     * @param fileType 文件类型，1-txt，2-doc，docx，3-xls，xlsx，4-pdf
     * @return
     * @throws TException
     */
    @Override
    public String file2ESByBinary(String fileName, String fileDir, String hostIps, String clusterName, String indexName, String typeName, int port, FileData fileData, int fileType) throws TException {
        String filePath;
        // 写到文件
            if (!fileDir.endsWith("/")) {
                filePath = fileDir + "/" + fileName;
            }else {
                filePath = fileDir + fileName;
                //filePath="E:\\freerch\\test数据\\pdf测试\\1.pdf";
            }
        try {
            File file = new File(filePath);
            FileOutputStream fos = new FileOutputStream(file);
            FileChannel channel = fos.getChannel();
            channel.write(fileData.buff);
            channel.close();
        }
        catch (Exception x)
        {
            x.printStackTrace();
        }


        try {
            client=ESUtils.getClient( hostIps,port,clusterName );
        } catch (Exception e) {
            e.printStackTrace();
            return "连接集群失败";
        }
        IndicesExistsResponse existsResponse = client.admin().indices().prepareExists( indexName ).execute().actionGet();
        if (!existsResponse.isExists()){
            ESUtils.createIndex( hostIps, port, clusterName, indexName, typeName );
        }
        String[] type=null;

        if (fileType==1){
            type=new String[]{"txt"};
        }else if (fileType==2){
            type=new String[]{"doc","docx"};
        }else if (fileType==3){
            type=new String[]{"xls","xlsx"};
        }else if (fileType==4){
            type=new String[]{"pdf"};
        }else {
            return "No filetype:"+fileType;
        }
        //File file=new File( filePath );
        //files( type, hostIps, clusterName, indexName, typeName, port, length, file );
        //Collection<File>  files = FileUtils.listFiles( new File( filePath ), null, true );

        //for (final File file:files){
            //加入索引
            XContentBuilder b;
            String md5;
            try {
                b=jsonBuilder().startObject();
                //String fileName = file.getName();
                b.field( "v1",fileName );
                //String absolutePath = file.getAbsolutePath();
                b.field( "v2",fileDir );
                //long lastModified = file.lastModified();
                String format = df.format( new Date(  ) );
                b.field( "v3",format );
                String key=fileDir+","+fileName+","+format;
                md5 = MD5Utils.md5( key );
                byte[] bytes = null;
                if (fileType==2){
                    String docOrDocx = OfficeUtils.ParseDocOrDocx( filePath );
                    bytes = docOrDocx.getBytes( "utf-8" );
                }else if (fileType==4){
                    String itextPdf2Txt = OfficeUtils.itextPdf2Txt( filePath );
                    bytes=itextPdf2Txt.getBytes("utf-8");
                }else if (fileType==3){
                    String poiExcel2Txt = OfficeUtils.poiExcel2Txt( filePath );
                    bytes = poiExcel2Txt.getBytes( "utf-8" );
                }else {
                    String fileEncode = FileEncode.getFileEncode( filePath );
                    if ("asci".equals( fileEncode )){
                        fileEncode="GBK";
                    }
                    bytes=FileUtils.readFileToString( new File( filePath ),fileEncode ).getBytes("utf-8");
                }
                    b.field( "v4" ,new String( new String( bytes,"utf-8").getBytes(),0,bytes.length,"utf-8" ) );
                    b.field( "analyzer","hanlp-index" );
                    b.field("search_analyzer", "hanlp-index");

                b.endObject();

                ESUtils.insert( hostIps,port,clusterName,indexName,typeName,md5,b);

            } catch (Exception e) {
                e.printStackTrace();
                // throw new RuntimeException(e.getMessage());
            }


       // }

        return "SUCCESS";

    }

    public static boolean isOSLinux() {
        Properties prop = System.getProperties();

        String os = prop.getProperty("os.name");
        if (os != null && os.toLowerCase().indexOf("windows") > -1) {
            return true;
        } else {
            return false;
        }
    }

    /*private void files(String fileType, String hostIps, String clusterName, String indexName, String typeName, int port, int length, File file) {
        if (file.isDirectory()){
            File[] files = file.listFiles();
            for (int i = 0; i <files.length ; i++) {
                File f = files[i];
                files( fileType, hostIps, clusterName,  indexName, typeName, port, length,f);
            }
        }else{

            XContentBuilder b;
            String md5;
            try {
                b=jsonBuilder().startObject();
                String fileName = file.getName();
                b.field( "v1",fileName );
                String absolutePath = file.getAbsolutePath();
                b.field( "v2",absolutePath );
                long lastModified = file.lastModified();
                String format = df.format( new Date( lastModified ) );
                b.field( "v3",format );
                String key=absolutePath+","+fileName+","+format;
                md5 = MD5Utils.md5( key );
                byte[] bytes = null;
                if (fileType==2){
                    String docOrDocx = OfficeUtils.ParseDocOrDocx( absolutePath );
                    bytes = docOrDocx.getBytes( "utf-8" );
                }else if (fileType==3){
                    String poiExcel2Txt = OfficeUtils.poiExcel2Txt( absolutePath );
                    bytes = poiExcel2Txt.getBytes( "utf-8" );
                }else if (fileType==4){
                    String itextPdf2Txt = OfficeUtils.itextPdf2Txt( absolutePath );
                    bytes=itextPdf2Txt.getBytes("utf-8");
                }else {
                    String fileEncode = FileEncode.getFileEncode( absolutePath );
                    if ("asci".equals( fileEncode )){
                        fileEncode="GBK";
                    }
                    bytes=FileUtils.readFileToString( file,fileEncode ).getBytes("utf-8");

                }
                if (length * 1024 <bytes.length){
                    b.field( "v4" ,new String( new String( bytes,"utf-8").getBytes(),0,bytes.length,"utf-8" ) );
                }else {
                    b.field( "v4",new String( bytes,"utf-8" ) );
                    System.out.println("new String(bytes) = " + new String(bytes,"utf-8"));
                }
                b.endObject();

                ESUtils.insert( hostIps,port,clusterName,indexName,typeName,md5,b);

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }*/

    private void insertdata(int fileType, String hostIps, String clusterName, String indexName, String typeName, int port, int length, File file) {

    }


    public static byte[] inputStream2Bytes(InputStream stream, String encoding) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(stream,encoding));
        String readLine = null;
        StringBuffer sb = new StringBuffer();
        while ((readLine = br.readLine()) != null) {
            sb.append(readLine).append("\n");
        }
        br.close();
        br = null;
        return sb.toString().getBytes();
    }
    private static BufferedReader readLineAndInsertEs(String hostIps, String indexName, String typeName,String clusterName, int port, String text) throws Exception {
        BufferedReader br;
        XContentBuilder b;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(text.getBytes());
        InputStreamReader inputStreamReader = new InputStreamReader(byteArrayInputStream,"utf-8");
        br = new BufferedReader(inputStreamReader);
        String readLine = null;
        int available = byteArrayInputStream.available();
        int count = 0;
//        Client client = DKSerachInput.getClient();
        //开启批量插入
//        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        while ((readLine = br.readLine()) != null) {
//            System.out.println("readLine = " + readLine);
            b = jsonBuilder().startObject();
            long timeMillis = System.currentTimeMillis();
            String md5Key = MD5Utils.md5(timeMillis + "");
            String[] split = readLine.split(",");
            for (int i = 0; i < split.length; i++) {
                b.field("v" + (i + 1), split[i]);
                b.field( "analyzer","hanlp" );
                b.field("search_analyzer", "hanlp");
            }
            b.endObject();

//            bulkRequestBuilder.add(client.prepareIndex(indexName,typeName,md5Key).setSource(b));
//            //每一千条提交一次
//            if (count% 1000==0) {
//                bulkRequestBuilder.execute().actionGet();
////                System.out.println("提交了：" + count);
//            }
//            count++;
            ESUtils.insert(hostIps, port,clusterName, indexName, typeName,md5Key, b); //索引名,类型名,id名
        }

//        if(available > 0) {
//            bulkRequestBuilder.execute().actionGet();
////            System.out.println("count = " + count);
//            System.out.println("插入完毕");
//        }
        return br;
    }

}
