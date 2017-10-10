/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 *
 * @author jkesanie
 */
@SpringBootApplication
@EnableRabbit
public class RMLService {

    public static final String SERVICE_NAME = "rmlservice";
    public static ObjectMapper mapper = new ObjectMapper();
    public static final String VERSION = "0.1";
    private static Logger log = Logger.getLogger(RMLService.class.toString());

    @Autowired
    private Environment env;


    String getPassword() {
        try {
            return env.getRequiredProperty("password");
        } catch (IllegalStateException iex) {
            log.severe("Missing required environmental property MPASS");
            return "password";
        }
        
    }

    String getUsername() {
        try {
            return env.getRequiredProperty("username");
        } catch (IllegalStateException iex) {
            log.severe("Missing required environmental property MUSER");
            return "user";
        }
        
    }

    String getExchangeName() {
        try {
            return env.getRequiredProperty("exchange");
        } catch (IllegalStateException iex) {
            log.severe("Missing required environmental property MEXCHANGE");
            return "";
        }
        
    }

    String getQueueName() {
        try {
            return env.getRequiredProperty("queue");
        } catch (IllegalStateException iex) {
            log.severe("Missing required environmental property MQUEUE");
            return SERVICE_NAME;
        }
        
    }

    URI getBrokerURI() {
        try {
            return new URI(env.getRequiredProperty("brokerURL"));
        } catch (URISyntaxException ex) {
            log.severe(ex.getMessage());
        } catch (IllegalStateException th) {
            log.info("Missing required environmental property MHOST");
            try {
                return new URI("amqp://localhost");
            } catch (URISyntaxException ex) {
                Logger.getLogger(RMLService.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }


    @Bean
    Queue queue() {
        return new Queue(getQueueName(), false);
    }

    @Bean
    DirectExchange exchange() {
        return new DirectExchange(getExchangeName(), true, true);
    }

    @Bean
    Binding binding() {
        return BindingBuilder.bind(queue()).to(exchange()).with(getQueueName());
    }

    @Bean
    ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(getBrokerURI());
        connectionFactory.setUsername(getUsername());
        connectionFactory.setPassword(getPassword());
        connectionFactory.setRequestedHeartBeat(10);
        return connectionFactory;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory myRabbitListenerContainerFactory() {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory());
        factory.setMaxConcurrentConsumers(1);
        factory.setRecoveryInterval(15000l);
        return factory;
    }


    public static void main(String[] args) throws InterruptedException {

        SpringApplication.run(RMLService.class, args);
    }
}
