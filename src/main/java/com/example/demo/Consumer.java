package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class Consumer {

    @KafkaListener(topics = "output-topic", groupId = "foo")
    public void listenGroupFoo(ConsumerRecord<String, String> record) {
        System.out.println("Received Message in group foo: " + record.key());
    }
}
