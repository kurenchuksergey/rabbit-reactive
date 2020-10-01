package com.kurenchuksergey.rabbitreactive;

import com.kurenchuksergey.rabbitreactive.helper.RabbitHelper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.http.client.domain.QueueInfo;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.*;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Component
public class Receiver {

    private final Mono<Connection> connectionMono;
    private final RabbitHelper rabbitHelper;
    private final String exchangeName = "testExchange";

    public Receiver(Mono<Connection> connectionMono, RabbitHelper rabbitHelper) {
        this.connectionMono = connectionMono;
        this.rabbitHelper = rabbitHelper;
        Sender sender = RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));

        sender.declareExchange(ExchangeSpecification.exchange(exchangeName)).block();
    }

    @PostConstruct
    public void init() {
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(v ->
                        scanExchange(exchangeName)
                                .flatMap(
                                        queueInfo ->
                                                createReceiver(queueInfo.getName())
                                                        .take(Duration.of(20, ChronoUnit.SECONDS))
                                )
                )
                .subscribe(delivery ->
                        System.out.println(new String(delivery.getBody()))
                );
    }


    public Flux<QueueInfo> scanExchange(String exchangeName) {
        return rabbitHelper
                .getQueuesByExchangeName(exchangeName)
                .filter(
                        queueInfo ->
                                queueInfo.getTotalMessages() > 0
                )
                .take(20);
    }


    public Flux<Delivery> createReceiver(String queueName) {
        reactor.rabbitmq.Receiver receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
        return receiver
                .consumeAutoAck(queueName);
    }
}
