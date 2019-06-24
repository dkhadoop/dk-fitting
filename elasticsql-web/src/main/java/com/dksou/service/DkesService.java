package com.dksou.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.dksou.essql.utils.CalciteUtil;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * Created by root on 2018/3/13.
 */
@Component
public class DkesService {
    private Properties properties = new Properties();

    @Autowired
    private RestTemplate restTemplate;

    @Value("${es.http.port}")
    String esHttpPort;

    public JSONObject query(String sql) throws FileNotFoundException {
        JSONObject queryResult = new JSONObject();
        final boolean[] isS = {true};
        List<String> resultList = new ArrayList<>();
        queryResult.put("succeed", false);
//        String jsonPath = "F:\\dk\\es\\刑侦\\dk-essql\\src\\main\\resources\\elasticsearch-zips-model3.json";
        File file = ResourceUtils.getFile("classpath:elasticsearch-zips-model3.json");
//        if (jsonPath.startsWith("file:"))
//            jsonPath = jsonPath.substring("file:".length());
        System.out.println("=======================" + file.getAbsolutePath());
        properties.put("model", file.getAbsolutePath());
        //in order to use '!=', otherwise we can just use '<>'
        properties.put("conformance", "ORACLE_10");
        //set this but useless. wonder why
        properties.put("caseSensitive", "false");
        System.out.println("sql: " + sql);
        CalciteUtil.execute(properties, sql, (Function<ResultSet, Void>) resultSet -> {
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int count = metaData.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    String columnName = metaData.getColumnName(i);
                    if (columnName.toLowerCase().endsWith(".keyword")) {
                        count = i - 1;
                    }
                }
                while (resultSet.next()) {
                    StringBuffer str = new StringBuffer();
                    for (int i = 1; i <= count; ++i) {
//                        str.append(resultSet.getObject(i).toString());
//                        System.out.print(metaData.getColumnLabel(i).toLowerCase() + ": " +
//                                (resultSet.getObject(i) != null ? resultSet.getObject(i).toString() : "null") + "    ");
                        str.append(metaData.getColumnLabel(i).toLowerCase()).append(": ").append(resultSet.getObject(i) != null ? resultSet.getObject(i).toString() : "null").append("    ");

                    }
//                    System.out.println();
                    resultList.add(str.toString());
                }
                queryResult.put("succeed", true);
            } catch (SQLException e) {
                e.printStackTrace();
                isS[0] = false;
            }

            return null;
        });
        if (isS[0]) {
            queryResult.put("succeed", true);
            queryResult.put("esdata", resultList);
            /*
             把resultList 保存进文件，便于下载结果用
             */
            queryResult.put("saveSucceed", saveResult(resultList, sql));
        } else {
            queryResult.put("succeed", false);
        }

        return queryResult;
    }

    /**
     * 将resultList 保存进文件,selectResult.txt
     *
     * @param resultList
     * @return
     */
    public JSONObject saveResult(List resultList, String sql) {
        JSONObject jsonObject = new JSONObject();
        try {
            File file = ResourceUtils.getFile("classpath:selectResult.txt");
            FileUtils.writeLines(file, "utf-8", resultList, "\n");
            jsonObject.put("isWrite", true);
            jsonObject.put("msg", "结果保存成功！");
        } catch (IOException e) {
            e.printStackTrace();
            jsonObject.put("isWrite", false);
            jsonObject.put("msg", "结果保存失败！");
        }

        return jsonObject;
    }

    /**
     * 保存es连接信息，生成json，保存到model3.json文件，
     * 同时用户输入信息保存一份到model2.json，用来做页面加载时展示用
     *
     * @param es_url
     * @param esCluster_name
     * @param esIndex_name
     * @return
     */
    public JSONObject saveSchema(String es_url, String esCluster_name, String esIndex_name) {
        JSONObject result = new JSONObject();
        /*
        判断 es是否可以连接
         */
        Settings settings = Settings.builder().put("cluster.name", esCluster_name)
                .put("client.transport.sniff", true)  // 开启嗅探，自动搜寻新加入集群IP
                .build();
        TransportClient client = null;
        if (es_url.contains(":") && StringUtils.split(es_url, ":").length == 2) {
            String[] esU = StringUtils.split(es_url, ":");
            String esIp = esU[0];
            int esPort = Integer.valueOf(esU[1]);
            try {
                if (!InetAddress.getByName(esIp).isReachable(2000)) {
                    result.put("succeed", false);
                    result.put("msg", "连接ip: " + esIp + "异常！");
                    return result;
                }
                client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esIp), esPort));
//                client.admin().cluster()
//                client.admin().indices().prepareGetIndex();
//                client.admin().indices().p

                IndicesExistsResponse indicesExistsResponse = client.admin().indices().exists(new IndicesExistsRequest().indices(esIndex_name)).actionGet();
                if (!indicesExistsResponse.isExists()) {
                    result.put("succeed", false);
                    result.put("msg", "索引" + esIndex_name + "不存在！");
                    return result;
                }
            } catch (IOException e) {
                e.printStackTrace();
                result.put("succeed", false);
                result.put("msg", "无法连接到ip: " + esIp);
                return result;
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        } else {
            result.put("succeed", false);
            result.put("msg", "连接es的地址输入格式有误！");
            return result;
        }

//        JSONObject json3 = JSONObject.parseObject(text);
        JSONObject json3 = new JSONObject(true);
        String esIp = es_url.split(":")[0];
        int esPort = Integer.valueOf(es_url.split(":")[1]);
        json3.put("esAddresses", "{'" + esIp + "':" + esPort + "}");
        json3.put("esClusterConfig", "{'cluster.name': '" + esCluster_name + "'}");
        json3.put("esIndexName", esIndex_name);

        JSONObject json4 = new JSONObject(true);
        json4.put("input1", es_url);
        json4.put("input2", esCluster_name);
        json4.put("input3", esIndex_name);


        JSONObject json1 = new JSONObject(true);

        JSONArray jr1 = new JSONArray();
        JSONObject json2 = new JSONObject();
        json2.put("type", "custom");
        json2.put("name", "test");
        json2.put("factory", "com.dksou.essql.ElasticsearchSchemaFactory");
        json2.put("operand", json3);


        jr1.add(json2);
        json1.put("version", "1.0");
        json1.put("defaultSchema", "test");
        json1.put("schemas", jr1);

        try {
            File file = ResourceUtils.getFile("classpath:elasticsearch-zips-model3.json");
            File file2 = ResourceUtils.getFile("classpath:elasticsearch-zips-model2.json");  //将原始数据存入 加载页面展示用
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(json1.toJSONString());

            FileWriter fw2 = new FileWriter(file2);
            BufferedWriter bw2 = new BufferedWriter(fw2);
            bw2.write(json4.toJSONString());

            bw2.flush();
            bw2.close();
            fw2.close();

            bw.flush();
            bw.close();
            fw.close();
            result.put("succeed", true);
//            JSONObject indexName = getIndexAndTypeName(esIp);
//            result.put("index", indexName);
        } catch (IOException e) {
            e.printStackTrace();
            result.put("succeed", false);
        }

        return result;
    }

    public JSONObject getEsConf() throws IOException {
        File file2 = ResourceUtils.getFile("classpath:elasticsearch-zips-model2.json");
        String s = FileUtils.readFileToString(file2, "utf-8");
        JSONObject jsonObject = JSONObject.parseObject(s);
        return jsonObject;
    }

    public JSONObject getIndexAndTypeName(String es_url, String esCluster_name) {
        JSONObject result = new JSONObject();
        ArrayList<JSONObject> indices = new ArrayList<>();
        if (es_url.contains(":") && StringUtils.split(es_url, ":").length == 2) {
            String[] esU = StringUtils.split(es_url, ":");
            String esIp = esU[0];
            int esPort = Integer.valueOf(esU[1]);
            Settings settings = Settings.builder().put("cluster.name", esCluster_name)
                    .put("client.transport.sniff", true)  // 开启嗅探，自动搜寻新加入集群IP
                    .build();
            TransportClient transportClient = null;
            try {
                transportClient = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esIp), esPort));
                ActionFuture<GetMappingsResponse> mappings = transportClient.admin().indices().getMappings(new GetMappingsRequest());
                Iterator<ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>>> iterator = mappings.actionGet().getMappings().iterator();
                while (iterator.hasNext()) {
                    ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> next = iterator.next();
                    String key = next.key;
                    ImmutableOpenMap<String, MappingMetaData> value = next.value;
                    Iterator<String> stringIterator = value.keysIt();
                    while (stringIterator.hasNext()) {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("index_name", key);
                        jsonObject.put("index_type", stringIterator.next());
                        indices.add(jsonObject);
                    }
                }
                result.put("succeed", true);
                result.put("indices", indices);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } finally {
                if (transportClient != null) {
                    transportClient.close();
                }
            }
        }else {
            result.put("succeed", false);
            result.put("msg", "连接es的地址输入格式有误！");
            return result;
        }

        /*if (es_url.contains(":") && StringUtils.split(es_url, ":").length == 2) {
            String[] esU = StringUtils.split(es_url, ":");
            String esIp = esU[0];
            ArrayList<JSONObject> indices = new ArrayList<>();
            JSONObject body = restTemplate.getForEntity("http://" + esIp + ":" + esHttpPort + "/_mappings", JSONObject.class).getBody();
            if (!body.isEmpty()) {
                for (String indexName : body.keySet()) {
                    JSONObject o = body.getJSONObject(indexName);
                    JSONObject mappings = o.getJSONObject("mappings");

                    for (String esType : mappings.keySet()) {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("index_name", indexName);
                        jsonObject.put("index_type", esType);
                        indices.add(jsonObject);
                    }
                }
                result.put("succeed", true);
                result.put("indices", indices);
            } else {
                result.put("succeed", false);
                result.put("errmsg", "未发现集群中的index！");
            }
        } else {
            result.put("succeed", false);
            result.put("msg", "连接es的地址输入格式有误！");
            return result;
        }*/
        return result;
    }

    /**
     * 下载查询结果
     *
     * @return
     */
    public ResponseEntity<FileSystemResource> getResultFile() {
        File file;
        try {
            file = ResourceUtils.getFile("classpath:selectResult.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        String format = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", "attachment; filename=" + format + ".txt");
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");
        return ResponseEntity
                .ok()
                .headers(headers)
                .contentLength(file.length())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(new FileSystemResource(file));
    }


}
