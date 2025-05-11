package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventsProducer;
import com.learnkafka.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    LibraryEventsProducer libraryEventsProducer;

    @Autowired
    ObjectMapper objectMapper;

    @TestConfiguration
    static class TestMockConfig {
        @Bean
        public LibraryEventsProducer libraryEventsProducer() {
            return Mockito.mock(LibraryEventsProducer.class);
        }
    }

    //Junit Test for
    @Test
    public void postLibraryEvent() throws Exception {
        //given - precondition or setup
        var payloadJson = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());
        when(libraryEventsProducer.sendLibraryEvents_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        //when  - action or behaviour that we are going to test
        var resultActions = mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payloadJson));

        //then - verify output
        resultActions.andExpect(MockMvcResultMatchers.status().isCreated());

    }

    //Junit Test for
    @Test
    public void postLibraryEvent_4xx() throws Exception {
        //given - precondition or setup
        var payloadJson = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());
        when(libraryEventsProducer.sendLibraryEvents_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        //when  - action or behaviour that we are going to test
        var resultActions = mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payloadJson));

        //then - verify output
        resultActions.andExpect(MockMvcResultMatchers.status().is4xxClientError());
    }

}