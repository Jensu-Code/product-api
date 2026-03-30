package com.jesus.course.springcloud.kafka.api.handlers;

import java.util.function.Consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import com.jesus.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.jesus.course.springcloud.kafka.api.models.dto.Reply;

@Configuration
public class RepliesConsumer {
    
    private final ReplyInbox replyInbox;
    
    public RepliesConsumer(ReplyInbox replyInbox) {
        this.replyInbox = replyInbox;
    }

    @Bean
    Consumer<Message<Reply<?>>> handleReplies() {
        return message -> {
            String correlationId = message.getHeaders().get("correlationId", String.class);
            System.out.println("Received reply with correlationId: " + correlationId);
            replyInbox.complete(correlationId, message.getPayload());
            // Process the reply message
            System.out.println("Received reply: " + message.getPayload());
        };
    }
}
