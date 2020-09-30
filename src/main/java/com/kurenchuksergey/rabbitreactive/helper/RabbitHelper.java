package com.kurenchuksergey.rabbitreactive.helper;

import com.rabbitmq.http.client.ReactorNettyClient;
import com.rabbitmq.http.client.domain.BindingInfo;
import com.rabbitmq.http.client.domain.QueueInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
public class RabbitHelper {
    @Autowired
    private ReactorNettyClient client;


    public Flux<QueueInfo> getQueuesByExchangeName(String exchangeName){
        Flux<BindingInfo> bindings = client.getExchangeBindingsBySource("/", exchangeName);
        Flux<QueueInfo> queue = bindings
                .filter(
                        bindingInfo -> bindingInfo.getDestinationType().equals("queue")
                )
                .map(
                        v -> v.getDestination())
                .flatMap(
                        name -> client.getQueue("/", name)
                );
        return queue;
    }



}
