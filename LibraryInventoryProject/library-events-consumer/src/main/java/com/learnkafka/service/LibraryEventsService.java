package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    LibraryEventsRepository repository;

    @Autowired
    ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Library Event :{}", libraryEvent);

        switch(libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
                break;
        }
    }

    private  void validate(LibraryEvent libraryEvent){
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        if(libraryEvent.getLibraryEventId() == 999){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        Optional<LibraryEvent> libraryEventOptional = repository.findById(libraryEvent.getLibraryEventId());
        if(libraryEventOptional.isEmpty()){
            throw new IllegalArgumentException("Not a valid library event");
        }
        log.info("Validation success for the library event {}", libraryEvent);
    }

    private  void save(LibraryEvent libraryEvent){
        //set LibraryEvent reference in Book Entity; one to one mapping
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        this.repository.save(libraryEvent);
    }
}
