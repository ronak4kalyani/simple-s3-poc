package com.simple.s3.poc;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SimpleS3Application {

	@Autowired
	private Service service;
	
	private static Service staticService ;
	
	@PostConstruct
	public void init() {
		staticService = service;
	}

	public static void main(String[] args) throws Exception {

		SpringApplication.run(SimpleS3Application.class); 
		
		staticService.serve();

	}

}
