package com.panxt.gmall0105.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.annotation.MapperScans;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.panxt.gmall0105.publisher.mapper")
public class Gmall0105PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0105PublisherApplication.class, args);
    }

}
