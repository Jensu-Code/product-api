package com.jesus.course.springcloud.kafka.api.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PingController {
   
    @GetMapping("/ping")
    public String ping() {
        return "ping product api";
    }
}
