package com.kurenchuksergey.rabbitreactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import reactor.tools.agent.ReactorDebugAgent;

@SpringBootApplication
public class RabbitReactiveApplication {

    public static void main(String[] args) {
        ReactorDebugAgent.init();
        ReactorDebugAgent.processExistingClasses();
        SpringApplication.run(RabbitReactiveApplication.class, args);
    }

}
