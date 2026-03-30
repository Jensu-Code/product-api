package com.jesus.course.springcloud.kafka.api.controllers;

import java.time.Duration;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.jesus.course.springcloud.kafka.api.models.dto.ProductDto;
import com.jesus.course.springcloud.kafka.api.models.dto.Reply;
import com.jesus.course.springcloud.kafka.api.services.ProductCommandService;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/products")
public class ProductController {
    
    private final ProductCommandService productCommandService;
    
    public ProductController(ProductCommandService productCommandService) {
        this.productCommandService = productCommandService;
    }
    
    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto product) {

        Reply<?> reply = productCommandService.sendCreateAndAwait(product, Duration.ofSeconds(5));
        if("SUCCESS".equalsIgnoreCase(reply.status())){
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }
}
