package com.example.demo.producer;

import com.example.demo.model.ConditionAtom;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConditionProducer {

    @Autowired
    void sendMessages(KafkaTemplate<String, ConditionAtom> kafkaTemplate) {
        var atom = new ConditionAtom();
        atom.field = "price";
        atom.value = 200;
        atom.operation = ">";

        for (int i = 0; i < 10000; i++) {
            atom.instrumentIdentifier = "inst" + (i % 10);
            atom.atomId = i;
            kafkaTemplate.send("condition", atom.instrumentIdentifier + atom.field, atom);
        }
    }
}
