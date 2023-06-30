package com.example.demo.processor;

import com.example.demo.model.AggregatedCondition;
import com.example.demo.model.ConditionAtom;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

@Component
public class ConditionAggregator {

    @Autowired
    public void configure(StreamsBuilder streamsBuilder) {
        var conditionSerDes = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(ConditionAtom.class));
        var aggregatedConditionSerDes = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(AggregatedCondition.class));

        var table = streamsBuilder.table("condition", Consumed.with(Serdes.String(), conditionSerDes));

        var stream = table
                .groupBy(KeyValue::new, Grouped.valueSerde(conditionSerDes))
                .aggregate(AggregatedCondition::new, (s, conditionAtom, conditionAtoms) -> {
                            conditionAtoms.add(conditionAtom);
                            return conditionAtoms;
                        }, (s, conditionAtom, conditionAtoms) -> conditionAtoms
                        , Materialized.with(Serdes.String(), aggregatedConditionSerDes)
                ).toStream();

        stream.to("aggregated-condition", Produced.valueSerde(aggregatedConditionSerDes));
    }
}
