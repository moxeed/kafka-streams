package com.example.demo.processor;

import com.example.demo.KafkaConfig;
import com.example.demo.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Properties;

@Component
public class RlcJoiner {

    @Autowired
    public void configure(StreamsBuilder streamsBuilder) {
        var rlcSerDes = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Rlc.class));
        var conditionSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Condition.class));
        var conditionAtomSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(ConditionAtom.class));
        var conditionEventSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(ConditionEvent.class));
        var aggregatedConditionSerDes = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(AggregatedCondition.class));

        var stream = streamsBuilder.stream("rlc", Consumed.with(Serdes.String(), rlcSerDes));
        var table = streamsBuilder.table("condition", Consumed.with(Serdes.String(), conditionAtomSerde));

        var aggregatedConditionKTable = table
                .groupBy(KeyValue::new, Grouped.valueSerde(conditionAtomSerde))
                .aggregate(AggregatedCondition::new, (s, conditionAtom, conditionAtoms) -> {
                            conditionAtoms.add(conditionAtom);
                            return conditionAtoms;
                        }, (s, conditionAtom, conditionAtoms) -> conditionAtoms
                        , Materialized.with(Serdes.String(), aggregatedConditionSerDes)
                );

        stream.join(aggregatedConditionKTable, (rlc, conditionAtoms) -> {
                    var result = new ArrayList<ConditionEvent>();

                    for (var atom : conditionAtoms) {
                        var event = new ConditionEvent();
                        event.conditionId = atom.conditionId;
                        event.atomId = atom.atomId;
                        event.operation = atom.operation;
                        event.atomValue = atom.value;
                        event.rlcValue = rlc.value;
                        result.add(event);
                    }

                    return result;
                })
                .flatMapValues((conditionAtoms) -> conditionAtoms)
                .groupBy((s, conditionEvent) -> conditionEvent.conditionId, Grouped.with(Serdes.Integer(), conditionEventSerde))
                .aggregate(Condition::new, (integer, conditionEvent, condition) -> {
                    condition.atoms.put(conditionEvent.atomId, conditionEvent.isOk());
                    return condition;
                }, Materialized.with(Serdes.Integer(), conditionSerde))
                .filter((integer, condition) -> condition.isPassed())
                .toStream()
                .to("matched-condition", Produced.with(Serdes.Integer(), conditionSerde));
    }
}
