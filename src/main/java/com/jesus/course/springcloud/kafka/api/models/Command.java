package com.jesus.course.springcloud.kafka.api.models;

public record Command<T>(CommandType type, Long id, T body) {

}
