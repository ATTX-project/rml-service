/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

/**
 *
 * @author jkesanie
 */
@SpringBootApplication
@EnableRabbit
@PropertySource("classpath:RMLService.properties")
public class RMLService {
    
    public static final String SERVICE_NAME = "rmlservice";
    
    @Autowired
    private Environment env;
    
    @Value("${default-broker-url}")
    private String defaultBrokerUrl;

    @Value("${default-exchange}")
    private String defaultExchange;

    @Value("${default-queue}")
    private String defaultQueue;

    String getExchangeName() {
        return env.getProperty("exchange", defaultExchange);
    }

    
    String getQueueName() {
        return env.getProperty("queue", defaultQueue);
    }

    String getBrokerURL() {
        return env.getProperty("brokerURL", defaultBrokerUrl);
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
    
    
    /*
    @Bean 
    MessageConverter messageConverter() {        
        Jackson2JsonMessageConverter c = new Jackson2JsonMessageConverter();
        DefaultClassMapper m = new DefaultClassMapper();
        m.setDefaultType(RMLServiceRequest.class);
        c.setClassMapper(m);
        return c;
    }
*/

    @Bean
    ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(getBrokerURL());
        connectionFactory.setUsername(env.getProperty("username", "user"));
        connectionFactory.setPassword(env.getProperty("password", "password"));
        connectionFactory.setRequestedHeartBeat(10);
        return connectionFactory;
    }
    
    @Bean
     public SimpleRabbitListenerContainerFactory myRabbitListenerContainerFactory() {
       SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
       factory.setConnectionFactory(connectionFactory());
       factory.setMaxConcurrentConsumers(1);
       
       return factory;
     }
   
    public static void main(String[] args) throws InterruptedException {
        
        SpringApplication.run(RMLService.class, args);
    }
}
