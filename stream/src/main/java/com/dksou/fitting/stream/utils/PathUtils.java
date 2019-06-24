package com.dksou.fitting.stream.utils;

import org.apache.log4j.Logger;

public class PathUtils {

    static Logger log = Logger.getLogger(PathUtils.class);
    public static String processingPathSeparator(String path){
        path = path.replaceAll("\\\\", "/");
        if (path.length() > 0 && !path.endsWith("/")) {
            path = path + "/";
        }
        return path;
    }

}
