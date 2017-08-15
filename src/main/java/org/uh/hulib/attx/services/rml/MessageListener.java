/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

/**
 *
 * @author jkesanie
 */

public class MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(MessageListener.class);    
    
    
    Transformer transformer;
    JmsMessagingTemplate template;
    
    @Autowired
    public void setTransformer(Transformer transformer) {
        this.transformer = transformer;
    }
    
    @Autowired
    public void setTemplate(JmsMessagingTemplate template) {
        this.template = template;
    }
    
    @JmsListener(destination = "${request-queue}")
    public void receive(TextMessage message, @Headers Map<String, Object> headers) throws Exception {
        //for(String h : headers.keySet()) {
          //  LOG.info(h);
        //}
        
        String correlationID = "";
        String replyTo = "";
        if(headers.containsKey("correlation-id"))
            correlationID = (String)headers.get("correlation-id");
        else if(headers.containsKey("jms_correlationId"))
            correlationID = (String)headers.get("jms_correlationId");

        if(headers.containsKey("reply-to")) {
            replyTo = (String)headers.get("reply-to");            
            LOG.info("ReplyTo: " + replyTo);
            if(replyTo.indexOf("://") >= 0) {
                replyTo = replyTo.substring(replyTo.indexOf("://") + 3);
                LOG.info("ReplyTo: " + replyTo);                
            }

        }
        else if(headers.containsKey("jms_replyTo")) {
            Destination destination = (Destination)headers.get("jms_replyTo");            
            replyTo = destination.toString();
            if(replyTo.indexOf("://") >= 0) {
                replyTo = replyTo.substring(replyTo.indexOf("://") + 3);
                LOG.info("ReplyTo: " + replyTo);                
            }            
        }
        LOG.info("received message='{}', correlationID='{}'", message.getText(), correlationID);
        URI output = transformer.transformToRDF(new URI("file://input"), "conf");
        
        if(!"".equals(replyTo)) {
            LOG.info("Create output: "+ output.toString());
            Map<String, Object> responseHeaders = new HashMap<String, Object>();
            responseHeaders.put("correlation-id", correlationID);
            template.convertAndSend(replyTo, output.toString(), responseHeaders);
            LOG.info("Send reply to: " + replyTo);
        }
        else {
            LOG.error("No reply to destination to send the results of the transformation");
        }
        
    }
    
}
