package com.example.demo.producer;

import com.example.demo.model.Rlc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RlcProducer {

    @Autowired
    void sendMessages(KafkaTemplate<String, Rlc> kafkaTemplate) {
        var rlc = new Rlc();
        rlc.field = "price";
        rlc.value = 300;
        for (int i = 0; i < 10000; i++) {
            rlc.instrumentNscId = "inst" + (i % 10);
            kafkaTemplate.send("rlc", rlc.instrumentNscId + rlc.field, rlc);
        }
    }
}
