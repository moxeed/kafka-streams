package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class Producer {

    @Autowired
    void sendMessages(KafkaTemplate<String, String> kafkaTemplate) {
        kafkaTemplate.send("input-topic", "hello there is some text ! hsjhdjs sdh jshj");
    }
}
