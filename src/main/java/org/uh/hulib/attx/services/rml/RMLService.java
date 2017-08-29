/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import javax.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsMessagingTemplate;

/**
 *
 * @author jkesanie
 */
@SpringBootApplication
@EnableJms
@PropertySource("classpath:RMLService.properties")
public class RMLService {
    
    public static final String defaultAgentID = "rmlservice";

    @Autowired
    private Environment env;
    
    @Value("${default-broker-url}")
    private String defaultBrokerUrl;

    
    @Bean
    public ActiveMQConnectionFactory activeMQConnectionFactory() {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();        
        activeMQConnectionFactory.setBrokerURL(env.getProperty("brokerURL", defaultBrokerUrl));

        return activeMQConnectionFactory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(activeMQConnectionFactory());
        factory.setConcurrency("3-10");
        return factory;
    }

    @Bean
    public MessageListener listener() {
        return new MessageListener();
    }
    
    @Bean 
    @Autowired
    public JmsMessagingTemplate jsmTemplate(ConnectionFactory connectionFactory) {
        JmsMessagingTemplate template = new JmsMessagingTemplate(connectionFactory);
        return template;
        
    }

    public static void main(String[] args) throws InterruptedException {
        
        SpringApplication.run(RMLService.class, args);
    }
}
