package com.jesus.course.springcloud.kafka.api.services;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import com.jesus.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.jesus.course.springcloud.kafka.api.models.Command;
import com.jesus.course.springcloud.kafka.api.models.CommandType;
import com.jesus.course.springcloud.kafka.api.models.dto.ProductDto;
import com.jesus.course.springcloud.kafka.api.models.dto.Reply;

@Service
public class ProductCommandServiceImp implements ProductCommandService {

    private final StreamBridge streamBridge;
    private final ReplyInbox replyInbox;

    public ProductCommandServiceImp(StreamBridge streamBridge, ReplyInbox replyInbox) {
        this.streamBridge = streamBridge;
        this.replyInbox = replyInbox;
    }

    @Override
    public Reply<?> sendCreateAndAwait(ProductDto product, Duration timeout) {
        return SentAndAwait(new Command<>(CommandType.CREATE, null, product), timeout);

    }

    @Override
    public Reply<?> sendReadAndAwait(Long id, Duration timeout) {
        return SentAndAwait(new Command<>(CommandType.READ, id, null), timeout);
    }
    
    @Override
    public Reply<?> sendReadAllAndAwait(Duration timeout) {
        return SentAndAwait(new Command<>(CommandType.READ_ALL, null, null), timeout);
    }

    @Override
    public Reply<?> sendUpdateAndAwait(Long id, ProductDto product, Duration timeout) {
        return SentAndAwait(new Command<>(CommandType.UPDATE, id, product), timeout);
    }

    @Override
    public Reply<?> sendDeleteAndAwait(Long id, Duration timeout) {
        return SentAndAwait(new Command<>(CommandType.DELETE, id, null), timeout);
    }

    private Reply<?> SentAndAwait(Command<ProductDto> command, Duration timeout) {
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<Reply<?>> future = replyInbox.register(correlationId);
        Message<Command<ProductDto>> msg = MessageBuilder.withPayload(command)
                .setHeader("correlationId", correlationId)
                .build();
        boolean sent = streamBridge.send("commands-out-0", msg);
        if (!sent) {
            throw new IllegalStateException("No se pudo enviar el comando a Kafka");
        }

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Timeout esperando respuesta de products-commands desde kafka");
        }
    }
}
