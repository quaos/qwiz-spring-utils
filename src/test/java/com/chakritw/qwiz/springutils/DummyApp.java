package com.chakritw.qwiz.springutils;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication(scanBasePackages = { "com.chakritw.qwiz.springutils" })
@Configuration
public class DummyApp
{
    public static void main( String[] args )
    {
        SpringApplication.run(DummyApp.class, args);
    }
}
