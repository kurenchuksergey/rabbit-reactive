package com.kurenchuksergey.rabbitreactive;

import com.kurenchuksergey.rabbitreactive.helper.RabbitHelper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.http.client.domain.QueueInfo;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

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
//        createReceiver("q1")
//
//                .delaySubscription(sender.declareQueue(QueueSpecification.queue("q1").durable(true)))
//                .subscribe(m->System.out.println(Arrays.toString(m.getBody())));
//        sender.declareExchange(ExchangeSpecification.exchange("testExchange"))
//                .thenMany(v ->
        Flux.interval(Duration.ofSeconds(20))
                .flatMap(v ->
                        scanExchange("testExchange")
                                .flatMap(
                                        queueInfo ->
                                                createReceiver(queueInfo.getName())
                                )
                ).checkpoint("testCheckpoint-1", true)
                .subscribe(delivery ->
                        System.out.println(Arrays.toString(delivery.getBody()))
                );
    }


    public Flux<QueueInfo> scanExchange(String exchangeName) {
        return rabbitHelper
                .getQueuesByExchangeName(exchangeName)
                .checkpoint("testCheckpoint0", true)
//                .filter(
//                        queueInfo ->
//                                queueInfo.getTotalMessages() > 0
//                )
                .checkpoint("testCheckpoint", true);
//                .take(20);
    }


    public Flux<Delivery> createReceiver(String queueName) {
        reactor.rabbitmq.Receiver receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
        return receiver
                .consumeAutoAck(queueName);
//                .take(Duration.of(10, ChronoUnit.SECONDS));
    }
}
