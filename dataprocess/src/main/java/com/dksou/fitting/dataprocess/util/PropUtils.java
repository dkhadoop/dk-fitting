package com.dksou.fitting.dataprocess.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropUtils {
	static Logger log = Logger.getLogger(PropUtils.class);
	private static Properties prop = new Properties();
	public static Properties getProp(String propName){
		InputStream resourceAsStream = PropUtils.class.getClassLoader().getResourceAsStream(propName);
		try {
			prop.load(resourceAsStream);
		} catch (IOException e) {
//			e.printStackTrace();
			log.error(e,e.fillInStackTrace());
		}
		return prop;
	}
}
