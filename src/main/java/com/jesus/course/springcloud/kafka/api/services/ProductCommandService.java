package com.jesus.course.springcloud.kafka.api.services;

import java.time.Duration;

import com.jesus.course.springcloud.kafka.api.models.dto.ProductDto;
import com.jesus.course.springcloud.kafka.api.models.dto.Reply;

public interface ProductCommandService {

    Reply<?> sendCreateAndAwait(ProductDto product, Duration timeout);
    Reply<?> sendReadAndAwait(Long id, Duration timeout);
    Reply<?> sendReadAllAndAwait(Duration timeout);
    Reply<?> sendUpdateAndAwait(Long id, ProductDto product, Duration timeout);
    Reply<?> sendDeleteAndAwait(Long id, Duration timeout);
}
