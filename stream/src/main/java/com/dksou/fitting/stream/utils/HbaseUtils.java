package com.dksou.fitting.stream.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class HbaseUtils {
    static Logger logger = Logger.getLogger(HbaseUtils.class);
    static Properties providerProp = PropUtils.getProp("consumer-hbase.properties");
    static String fgf =  providerProp.getProperty("consumer.hbase.kafkaMessage.separator");
    public static Configuration configuration;
    private static Admin admin = null;
    private static Random random = null;//生成主键使用
    private static Connection connection = null;
    static {
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", providerProp.getProperty("consumer.hbase.zookeeper.connect"));
            //configuration.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            random = new Random();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     */
    public static void createTable(String tableName,String cf) {
        //logger.info("start create table ......");
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
//                admin.disableTable(tn);
//                admin.deleteTable(tn);
               logger.info(tableName + " is exist,detele....");
            }else{
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        System.out.println("end create table ......");
    }

    /**
     * 插入数据
     *
     * @param tableName
     */
    public static void insertData(String tableName,String cf,String data) {
        // System.out.println("start insert data ......");
        Table table = null;
        TableName tn = TableName.valueOf(tableName);
        try {
            table = connection.getTable(tn);
            // System.out.println("init insert data ......");
            Put put = new Put(String.valueOf(System.currentTimeMillis() +"" + random.nextLong()).getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
//            put.addColumn("column1".getBytes(), null, "ddd".getBytes());// 本行数据的第一列
//            put.addColumn("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
//            put.addColumn("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列

            String[] split = data.split(fgf);
            for(int i = 0; i < split.length ; i++){
                put.addColumn(cf.getBytes(),("v"+ (i + 1 )).getBytes(),split[i].getBytes());
            }
            // System.out.println("insert data ......");
            table.put(put);
            // System.out.println("insert data over......");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        // System.out.println("end insert data ......");
    }

    /**
     * 删除一张表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) {
        try {
            TableName tn = TableName.valueOf(tableName);
            admin.disableTable(tn);
            admin.deleteTable(tn);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * 根据 rowkey删除一条记录
     *
     * @param tablename
     * @param rowkey
     */
    public static void deleteRow(String tablename, String rowkey) {
        Table table = null;
        TableName tn = TableName.valueOf(tablename);
        try {
            table = connection.getTable(tn);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);
            table.delete(list);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * 组合条件删除
     *
     * @param tablename
     * @param rowkey
     */
    public static void deleteByCondition(String tablename, String rowkey) {
        // 目前还没有发现有效的API能够实现 根据非rowkey的条件删除 这个功能能，还有清空表全部数据的API操作

    }

    /**
     * 查询所有数据
     *
     * @param tableName
     */
    public static void QueryAll(String tableName) {
        Table table = null;
        TableName tn = TableName.valueOf(tableName);
        try {
            table = connection.getTable(tn);
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell cell : r.rawCells()) {
                    System.out.println("列：" + new String(cell.getFamilyArray())
                            + "====值:" + new String(cell.getValueArray()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录
     *
     * @param tableName
     */
    public static void QueryByCondition1(String tableName) {

        Table table = null;
        TableName tn = TableName.valueOf(tableName);
        try {
            table = connection.getTable(tn);
            Get scan = new Get("112233bbbcccc".getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (Cell cell : r.rawCells()) {
                System.out.println("列：" + new String(cell.getFamilyArray())
                        + "====值:" + new String(cell.getValueArray()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * 单条件按查询，查询多条记录
     *
     * @param tableName
     */
    public static void QueryByCondition2(String tableName) {

        Table table = null;
        TableName tn = TableName.valueOf(tableName);
        try {
            table = connection.getTable(tn);
            Filter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("column1"), null, CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("ddd")); // 当列column1的值为ddd时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell cell : r.rawCells()) {
                    System.out.println("列：" + new String(cell.getFamilyArray())
                            + "====值:" + new String(cell.getValueArray()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    /**
     * 组合条件查询
     *
     * @param tableName
     */
    public static void QueryByCondition3(String tableName) {

        Table table = null;
        TableName tn = TableName.valueOf(tableName);
        try {
            table = connection.getTable(tn);
            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(
                    Bytes.toBytes("column1"), null, CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("aaa"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(
                    Bytes.toBytes("column2"), null, CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("bbb"));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(
                    Bytes.toBytes("column3"), null, CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("ccc"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell cell : r.rawCells()) {
                    System.out.println("列：" + new String(cell.getFamilyArray())
                            + "====值:" + new String(cell.getValueArray()));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }


    public static class ImportThread extends Thread {
        private static Random random = null;//生成主键使用
        private static Connection connection = null;
        public void HandleThread() {
            // this.TableName = "T_TEST_1";

        }

        //
        public void run() {
            try {
                InsertProcess("test");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                System.gc();
            }
        }
    }

    /*
     * 多线程环境下线程插入函数
     */
    public static void InsertProcess(String tableName) throws IOException {
        // System.out.println("start insert data ......");
        Table table = null;
        TableName tn = TableName.valueOf(tableName);
        int count = 15000;
        long start = System.currentTimeMillis();
        try {
            table = connection.getTable(tn);
            List<Put> list = new ArrayList<Put>();
            Put put = null;
            for(int i=0;i<count;i++) {
                // System.out.println("init insert data ......");
                put = new Put(String.valueOf(random.nextLong()).getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
                put.addColumn("column1".getBytes(), null, "ddd".getBytes());// 本行数据的第一列
                put.addColumn("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
                put.addColumn("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列
                // System.out.println("insert data ......");
                list.add(put);
            }
            table.put(list);
            long stop = System.currentTimeMillis();
            System.out.println("线程:"+Thread.currentThread().getId()+"插入数据："+count+"共耗时："+ (stop - start)*1.0/1000+"s");
            // System.out.println("insert data over......");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /*
     * Mutil thread insert test
     */
    public static void MultThreadInsert() throws InterruptedException {
        System.out.println("---------开始MultThreadInsert测试----------");
        long start = System.currentTimeMillis();
        int threadNumber = 5;
        Thread[] threads = new Thread[threadNumber];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new ImportThread();
            threads[i].start();
        }
        for (int j = 0; j < threads.length; j++) {
            (threads[j]).join();
        }
        long stop = System.currentTimeMillis();

        System.out.println("MultThreadInsert：" + threadNumber * 10000 + "共耗时："
                + (stop - start) * 1.0 / 1000 + "s");
        System.out.println("---------结束MultThreadInsert测试----------");
    }

}
