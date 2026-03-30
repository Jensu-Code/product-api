package com.jesus.course.springcloud.kafka.api.messaging;

import org.springframework.stereotype.Component;

import com.jesus.course.springcloud.kafka.api.models.dto.Reply;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ReplyInbox {
   
    private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>> pending = new ConcurrentHashMap<>();

    public CompletableFuture<Reply<?>> register(String correlationId) {
       CompletableFuture<Reply<?>> future = new CompletableFuture<>();
       pending.put(correlationId, future);
       return future;
    }
    
    public void complete(String correlationId, Reply<?> reply) {
        if (correlationId == null){
            throw new NullPointerException("correlationId cannot be null");
        }
        CompletableFuture<Reply<?>> future = pending.remove(correlationId);
        if (future != null) {
            future.complete(reply);
        }
    }
}
