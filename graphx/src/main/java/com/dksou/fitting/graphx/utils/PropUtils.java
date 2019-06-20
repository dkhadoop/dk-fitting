package com.dksou.fitting.graphx.utils;


import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class PropUtils {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PropUtils.class);
	private static Properties prop = new Properties();
	public static Map<String,String> getProp(String propName){
		Map<String, String> map = new HashMap<String, String>();
		InputStream resourceAsStream = PropUtils.class.getClassLoader().getResourceAsStream(propName);
		try {
			prop.load(resourceAsStream);

			Enumeration<?> enu = prop.propertyNames();
			while (enu.hasMoreElements()) {
				String key = (String) enu.nextElement();
				String value = prop.getProperty(key);
				if(!"".equals(value) && value != null){
					if(!key.contains("port")){
						value = PathUtils.processingPathSeparator(value);
					}
				}

				map.put(key, value);
			}
		} catch (IOException e) {
//			e.printStackTrace();
			logger.error(e.getMessage(),e);
		}finally {
			try {
				resourceAsStream.close();
			} catch (IOException e) {
//				e.printStackTrace();
				logger.error(e.getMessage(),e);
			}
		}
		return map;
	}

	public static void main(String[] args) {
		Map<String, String> prop = PropUtils.getProp("dkgraphx.properties");

		Set<String> strings = prop.keySet();
		Iterator<String> iterator = strings.iterator();
		while (iterator.hasNext()){
			String next = iterator.next();
			String s = prop.get(next);
			System.out.println( next + " -> " + s);
		}

		System.out.println("driver_memory".toUpperCase());
		System.out.println("num_executors".toUpperCase());
		System.out.println("executor_memory".toUpperCase());
		System.out.println("executor_cores".toUpperCase());
	}
}
