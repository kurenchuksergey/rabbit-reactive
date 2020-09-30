package com.kurenchuksergey.rabbitreactive;

import com.kurenchuksergey.rabbitreactive.helper.RabbitHelper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.http.client.domain.QueueInfo;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import reactor.rabbitmq.Sender;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

@Component
public class Receiver {

    private final Mono<Connection> connectionMono;
    private final RabbitHelper rabbitHelper;

    public Receiver(Mono<Connection> connectionMono, RabbitHelper rabbitHelper) {
        this.connectionMono = connectionMono;
        this.rabbitHelper = rabbitHelper;
        Sender sender = RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
        sender.declareExchange(ExchangeSpecification.exchange("testExchange"))
                .flatMapMany(v ->
                        Flux.interval(Duration.ofSeconds(20)))
                .flatMap(v ->
                        scanExchange("testExchange")
                                .switchMap(
                                        queueInfo ->
                                                createReceiver(queueInfo.getName())
                                )
                ).checkpoint("testCheckpoint-1")
                .subscribe(delivery ->
                        System.out.println(Arrays.toString(delivery.getBody()))
                );
    }


    public Flux<QueueInfo> scanExchange(String exchangeName) {
        return rabbitHelper
                .getQueuesByExchangeName(exchangeName)
                .checkpoint("testCheckpoint0")
                .filter(
                        queueInfo ->
                                queueInfo.getTotalMessages() > 0
                )
                .checkpoint("testCheckpoint")
                .take(20);
    }


    public Flux<Delivery> createReceiver(String queueName) {
        reactor.rabbitmq.Receiver receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
        return receiver
                .consumeNoAck(queueName)
                .take(Duration.of(10, ChronoUnit.SECONDS));
    }
}
