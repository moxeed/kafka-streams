package com.example.demo.processor;

import com.example.demo.model.AggregatedCondition;
import com.example.demo.model.ConditionAtom;
import com.example.demo.model.Rlc;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

@Component
public class RlcJoiner {

    @Autowired
    public void configure(StreamsBuilder streamsBuilder) {
        var rlcSerDes = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Rlc.class));
        var conditionSerDes = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(ConditionAtom.class));
        var aggregatedConditionSerDes = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(AggregatedCondition.class));

        var table = streamsBuilder.table("aggregated-condition", Consumed.with(Serdes.String(), aggregatedConditionSerDes));
        var stream = streamsBuilder.stream("rlc", Consumed.with(Serdes.String(), rlcSerDes));

        stream.join(table, (rlc, conditionAtoms) -> conditionAtoms)
                .flatMapValues((conditionAtoms) -> conditionAtoms)
                .to("matched-condition", Produced.valueSerde(conditionSerDes));
    }
}
