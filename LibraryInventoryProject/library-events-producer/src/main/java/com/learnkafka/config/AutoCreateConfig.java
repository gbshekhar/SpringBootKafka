package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateConfig {

    @Value("${spring.kafka.topic}")
    public String topic;

    //Below method is used to create the Topic automatically
    //we just need to return Bean type NewTopic and method name can be any.
    @Bean
    public NewTopic createTopicLibraryEvents(){
        return TopicBuilder
                .name(topic)
                .partitions(3)
                .replicas(3)
                .build();
    }
}
