package com.kurenchuksergey.rabbitreactive;

import com.kurenchuksergey.rabbitreactive.helper.RabbitHelper;
import com.rabbitmq.client.Connection;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import javax.annotation.PostConstruct;
import java.time.Duration;

@Component
public class Sender {

    private final Mono<Connection> connectionMono;
    private final RabbitHelper rabbitHelper;
    private final reactor.rabbitmq.Sender sender;
    private final String exchangeName = "testExchange";


    public Sender(Mono<Connection> connectionMono, RabbitHelper rabbitHelper) {
        this.connectionMono = connectionMono;
        this.rabbitHelper = rabbitHelper;
        sender = RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
        sender.declareExchange(ExchangeSpecification.exchange(exchangeName)).block();
    }

    @PostConstruct
    public void init() {
        Flux.range(1, 50)
                .flatMap(i ->
                        sender.declare(QueueSpecification.queue("q" + i)
                                .exclusive(true))
                )
                .flatMap(a ->
                        sender.bind(BindingSpecification.binding(exchangeName, a.getQueue(), a.getQueue()))
                )
                .thenMany(
                        Flux.interval(Duration.ofSeconds(1)))
                .flatMap(i -> Flux.range(1, 50))
                .map(i -> "q" + i)
                .flatMap(queueName ->
                        sender.send(
                                Flux.range(1, 1000)
                                        .map(i -> new OutboundMessage(exchangeName, queueName, ("Message_" + i).getBytes()))
                        )
                ).subscribe();
    }


}
