/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URL;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceInput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequest;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponse;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Context;

/**
 *
 * @author jkesanie
 */
@Component
public class RMLServiceMessageListener {

    private static Logger log = Logger.getLogger(RMLServiceMessageListener.class.toString());
    
    private ObjectMapper mapper = new ObjectMapper();
    
    @Autowired
    RMLIOTransformer transformer;

    @Autowired
    RabbitTemplate template;

    @Autowired
    Queue queue;

    @Autowired
    private Environment env;

    public String getAgentID() {
        return env.getProperty("agentID", RMLService.SERVICE_NAME);
    }

    @Bean
    public String getQueueName() {
        return queue.getName();
    }

    @RabbitListener(queues = "#{getQueueName}")
    public void receive(Message message) {
        log.log(Level.INFO, "Received message -");
        String correlationID = message.getMessageProperties().getCorrelationIdString();
        String replyTo = message.getMessageProperties().getReplyTo();
        log.log(Level.INFO, "correlationID:" + correlationID);
        log.log(Level.INFO, "ReplyTo:" + replyTo);
        try {
            RMLServiceRequest request = null;
            try {
                String requestID = (correlationID != null ? correlationID : UUID.randomUUID().toString());
                String messageStr = new String(message.getBody(), "UTF-8");
                request = RMLService.mapper.readValue(messageStr, RMLServiceRequest.class);
                RMLServiceResponse response = transformer.transform(request, requestID);
                
                String responseStr = mapper.writeValueAsString(response);
                if(response != null) {
                    log.log(Level.INFO, "Response status:" + response.getPayload().getStatus());
                    log.log(Level.INFO, response.getPayload().getTransformedDatasetURL());
                }
                else {
                    log.log(Level.INFO, "Response was null");
                }
                if (correlationID == null) {
                    log.log(Level.INFO, "Sending response without correlation ID to " + replyTo);                    
                    template.convertAndSend(replyTo, responseStr);
                } else {
                    template.convertAndSend(replyTo, responseStr);
                }
            } catch (Exception ex) {
                log.log(Level.SEVERE, "Sending basic reply failed.", ex);
                // TODO: what if request is null?                
                Context ctx = request.getProvenance().getContext();
                String errorResponse = "{\n"
                    + "	\"provenance\": {\n"
                    + "		\"context\": {\n"
                    + "			\"workflowID\": \"" + ctx.getWorkflowID() + "\",\n"
                    + "			\"activityID\": \"" + ctx.getActivityID()+ "\",\n"
                    + "			\"stepID\": \"" + ctx.getStepID()+ "\"\n"
                    + "		}\n"
                    + "	},"
                    + "    \"payload\": {\n"
                    + "        \"contentType\": \"application/n-triples\",\n"
                    + "        \"status\": \"ERROR\",\n"
                    + "        \"statusMessage\": \"" + ex.getMessage() + "\",\n"
                    + "        \"transformedDatasetURL\": null\n"
                    + "    }\n"
                    + "}";
                
                byte[] body = errorResponse.getBytes();
                MessageProperties props = new MessageProperties();
                if (correlationID != null) {
                    props.setCorrelationIdString(correlationID);
                }
                props.setReplyTo(replyTo);
                Message responseMessage = new Message(body, props);                
                template.send(replyTo, responseMessage);
            }
        } catch (Exception ex) {
            // TODO: handle message sending error
            ex.printStackTrace();
        }

    }

}
