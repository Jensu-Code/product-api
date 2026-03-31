package com.jesus.course.springcloud.kafka.api.controllers;

import java.time.Duration;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    
    @GetMapping
    public ResponseEntity<?> readAll() {
        Reply<?> reply = productCommandService.sendReadAllAndAwait(Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }
    
    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto product) {

        Reply<?> reply = productCommandService.sendCreateAndAwait(product, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> read(@PathVariable Long id) {
        Reply<?> reply = productCommandService.sendReadAndAwait(id, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @PatchMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable Long id, @Valid @RequestBody ProductDto product) {
        Reply<?> reply = productCommandService.sendUpdateAndAwait(id, product, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        Reply<?> reply = productCommandService.sendDeleteAndAwait(id, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    private ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if("SUCCESS".equalsIgnoreCase(reply.status())){
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }
}
