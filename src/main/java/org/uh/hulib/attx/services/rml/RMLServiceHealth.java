/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import com.rabbitmq.client.Channel;
import java.util.Map;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

/**
 *
 * @author jkesanie
 */
@Component
public class RMLServiceHealth  extends AbstractHealthIndicator {

    @Autowired
    RabbitTemplate template;

    public RMLServiceHealth() {
        System.out.println("Health created");
    }
    
    private String getVersion() {
        return this.template.execute(new ChannelCallback<String>() {
            @Override
            public String doInRabbit(Channel channel) throws Exception {
                Map<String, Object> serverProperties = channel.getConnection()
                        .getServerProperties();
                return serverProperties.get("version").toString();
            }
        });
    }

    @Override
    protected void doHealthCheck(Health.Builder b) throws Exception {
        try {
            b.up();
        }catch(Exception ex) {
            b.down();
        }
        
    }
    
   
}
