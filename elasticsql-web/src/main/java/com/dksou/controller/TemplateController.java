package com.dksou.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * Created by root on 2018/3/13.
 */
@Controller
public class TemplateController {
    @RequestMapping(value = "/",method = RequestMethod.GET)
    public String indexShow() {
        return "index2.html";
    }
}
