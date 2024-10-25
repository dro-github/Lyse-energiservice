package com.cts;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling

public class StartPoint {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(StartPoint.class, args);
        while (true) {
            Thread.sleep(1000);
        }
    }
}
