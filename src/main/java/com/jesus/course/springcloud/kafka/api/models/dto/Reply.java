package com.jesus.course.springcloud.kafka.api.models.dto;

import com.jesus.course.springcloud.kafka.api.models.ReplyStatus;

public record Reply<T>(ReplyStatus status, String message, T body) {

}
