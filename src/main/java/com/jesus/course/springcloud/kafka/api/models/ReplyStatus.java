package com.jesus.course.springcloud.kafka.api.models;

public enum ReplyStatus {
    SUCCESS,
    ERROR;
    
    public boolean isSuccess() {
        return this == SUCCESS;
    }
}
