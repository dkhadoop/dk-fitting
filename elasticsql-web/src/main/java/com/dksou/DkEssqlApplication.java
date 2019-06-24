package com.dksou;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;


import java.util.HashMap;

@SpringBootApplication
public class DkEssqlApplication  extends SpringBootServletInitializer { //extends SpringBootServletInitializer

	/*protected SpringApplicationBuilder configure(
			SpringApplicationBuilder application) {
		return application.sources(DkEssqlApplication.class);
	}*/

	public static void main(String[] args) {
		SpringApplication.run(DkEssqlApplication.class, args);
	}

}
