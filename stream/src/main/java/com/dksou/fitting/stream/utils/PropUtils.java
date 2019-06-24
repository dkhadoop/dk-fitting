package com.dksou.fitting.stream.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
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

	public static Map<String, String> getMapProp(String propName) {
		Map<String, String> map = new HashMap<String, String>();
		InputStream resourceAsStream = PropUtils.class.getClassLoader().getResourceAsStream( propName );
		try {
			prop.load( resourceAsStream );

			Enumeration<?> enu = prop.propertyNames();
			while (enu.hasMoreElements()) {
				String key = (String) enu.nextElement();
				String value = prop.getProperty( key );
				if (!"".equals( value ) && value != null) {
					if (!key.contains( "port" )) {
						value = PathUtils.processingPathSeparator( value );
					}
				}

				map.put( key, value );
			}
		} catch (IOException e) {
//			e.printStackTrace();
			log.error( e.getMessage(), e );
		} finally {
			try {
				resourceAsStream.close();
			} catch (IOException e) {
//				e.printStackTrace();
				log.error( e.getMessage(), e );
			}
		}
		return map;
	}

}
