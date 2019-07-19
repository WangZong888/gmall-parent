package com.wsw.gmall.logger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GmallLoggerApplication {

    //只能扫描在同一个包下的java文件
    public static void main(String[] args) {
        SpringApplication.run(GmallLoggerApplication.class, args);
    }

}
