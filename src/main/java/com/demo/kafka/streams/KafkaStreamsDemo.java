package com.demo.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Arrays;

@EnableKafkaStreams
@SpringBootApplication
public class KafkaStreamsDemo {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsDemo.class, args);
    }

    @Bean
    // StreamsBuilder, который используется для создания потока Kafka Streams.
    public KStream<String, Long> kStreamWordCounter(StreamsBuilder streamsBuilder) {
        final KStream<String, Long> wordCountStream = streamsBuilder
                //создает поток данных Kafka из топика words. Метод Consumed.with(Serdes.String(), Serdes.String()) указывает,
                // каким образом данные из темы должны быть десериализованы. В данном случае, ключи и значения десериализуются с использованием
                // Serdes.String(), что означает, что они являются строками.
                .stream("devglan-pertitions-topic", Consumed.with(Serdes.String(), Serdes.String()))
                // Это позволяет рассматривать каждое слово в качестве отдельной записи.
                .flatMapValues(word -> Arrays.asList(word.split(" ")))
                // принимает каждую запись в потоке и преобразует ее в новую запись, где ключом и значением является слово. Это необходимо для группировки и подсчета слов.
                .map(((key, value) -> new KeyValue<>(value, value)))
                // группирует записи в потоке по ключу.
                .groupByKey()
                // подсчитывает количество записей в каждой группе
                .count()
                //преобразует результаты подсчета в новый поток данных Kafka
                .toStream();
        wordCountStream.to("word-counters", Produced.with(Serdes.String(), Serdes.Long()));
        return wordCountStream;
    }
}