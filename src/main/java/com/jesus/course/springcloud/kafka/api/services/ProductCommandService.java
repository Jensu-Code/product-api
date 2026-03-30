package com.jesus.course.springcloud.kafka.api.services;

import java.time.Duration;

import com.jesus.course.springcloud.kafka.api.models.dto.ProductDto;
import com.jesus.course.springcloud.kafka.api.models.dto.Reply;

public interface ProductCommandService {

    Reply<?> sendCreateAndAwait(ProductDto product, Duration timeout);
}
