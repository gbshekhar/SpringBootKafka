package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper){
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    //Approach-1: Asynchronous
    public CompletableFuture<SendResult<Integer, String>>  sendLibraryEvents(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var data = objectMapper.writeValueAsString(libraryEvent);

        //1.Blocking call - get metadata about the Kafka Cluster; max time it can wait to receive response is max.block.ms and default value is 6seconds
        //Note: This call happens only one time after the server starts
        //2.Send message happens - Returns a CompletableFuture; in case if this call fails based on retires property it will try that many times
        var completableFuture = kafkaTemplate.send(topic, key, data);
        return completableFuture.whenComplete((sendResult, throwable) -> {
             if(throwable != null){
                 handleFailure(key, data, throwable);
             } else{
                 handleSuccess(key, data, sendResult);
             }
        });
    }

    //Approach-2: synchronous
    public SendResult<Integer, String>  sendLibraryEvents_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException,
            ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var data = objectMapper.writeValueAsString(libraryEvent);

        //1.Blocking call - get metadata about the Kafka Cluster
        //Note: This call happens only one time after the server starts
        //2.Send message happens - Block and wait until message sent to kafka
        var sendResult = kafkaTemplate.send(topic, key, data)
                //.get();
                        .get(3, TimeUnit.SECONDS);
        handleSuccess(key, data, sendResult);
        return sendResult;
    }

    //Approach-3: Asynchronous - but using ProducerRecord instead of sending key and value directly
    public CompletableFuture<SendResult<Integer, String>>  sendLibraryEvents_Approach3(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var data = objectMapper.writeValueAsString(libraryEvent);

        //Actually we can send ProducerRecord instead of topic, key, data
        var ProducerRecord = createProducerRecord(key, data);
        //1.Blocking call - get metadata about the Kafka Cluster
        //Note: This call happens only one time after the server starts
        //2.Send message happens - Returns a CompletableFuture
        var completableFuture = kafkaTemplate.send(topic, key, data);
        return completableFuture.whenComplete((sendResult, throwable) -> {
            if(throwable != null){
                handleFailure(key, data, throwable);
            } else{
                handleSuccess(key, data, sendResult);
            }
        });
    }

    private ProducerRecord createProducerRecord(Integer key, String data) {
        List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, data, headers);
    }

    private void handleSuccess(Integer key, String data, SendResult<Integer, String> sendResult) {
        log.info("Message Send successfully for the key: {} and the value :{}, partition is {}", key, data, sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String data, Throwable throwable) {
        log.error("Error sending the message and the exceptions is {}", throwable.getMessage());
    }
}
