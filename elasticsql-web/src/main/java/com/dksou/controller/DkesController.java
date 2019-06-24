package com.dksou.controller;

import com.alibaba.fastjson.JSONObject;
import com.dksou.service.DkesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by root on 2018/3/13.
 */
@RestController
public class DkesController {

    @Autowired
    DkesService dkesService;

    @RequestMapping("/test")
    public String test() {
        return "hello!!!!";
    }

    @RequestMapping(value = "/es", method = RequestMethod.POST)
    public JSONObject query(String sql) throws FileNotFoundException {
        return dkesService.query(sql);
    }


    @RequestMapping(value = "/schema", method = RequestMethod.POST)
    public JSONObject saveSchema(String es_url, String esCluster_name, String esIndex_name) {
        return dkesService.saveSchema(es_url, esCluster_name, esIndex_name);
    }

    @RequestMapping(value = "/es_conf", method = RequestMethod.GET)
    public JSONObject getEsConf() throws IOException {
        return dkesService.getEsConf();
    }

    @RequestMapping(value = "/result_file", method = RequestMethod.GET)
    public ResponseEntity<FileSystemResource> listExport() {
        return dkesService.getResultFile();
    }

    @RequestMapping(value = "/index", method = RequestMethod.POST)
    public JSONObject getIndex(String es_url, String esCluster_name) {
        return dkesService.getIndexAndTypeName(es_url,esCluster_name);
    }
}
