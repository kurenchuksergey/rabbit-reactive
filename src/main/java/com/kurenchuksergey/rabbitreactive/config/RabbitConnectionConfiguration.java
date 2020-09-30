package com.kurenchuksergey.rabbitreactive.config;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.http.client.ReactorNettyClient;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class RabbitConnectionConfiguration {

    @Bean()
    Mono<Connection> connectionMono(RabbitProperties rabbitProperties) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitProperties.getHost());
        connectionFactory.setPort(rabbitProperties.getPort());
        connectionFactory.setUsername(rabbitProperties.getUsername());
        connectionFactory.setPassword(rabbitProperties.getPassword());
        return Mono.fromCallable(() -> connectionFactory.newConnection("reactor-rabbit")).cache();
    }


    @Bean
    ReactorNettyClient apiClient(RabbitProperties rabbitProperties) {
        return new ReactorNettyClient("http://" + rabbitProperties.getHost() + ":" + 15672 + "/api",
                rabbitProperties.getUsername(),
                rabbitProperties.getPassword());
    }
}
