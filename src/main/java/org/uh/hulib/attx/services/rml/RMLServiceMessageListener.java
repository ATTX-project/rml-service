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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
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
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequestMessage;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponseMessage;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Context;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Provenance;

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
        OffsetDateTime startTime = OffsetDateTime.now();
        log.log(Level.INFO, "Received message -");
        String correlationID = message.getMessageProperties().getCorrelationIdString();
        String replyTo = message.getMessageProperties().getReplyTo();
        log.log(Level.INFO, "correlationID:" + correlationID);
        log.log(Level.INFO, "ReplyTo:" + replyTo);
        
        try {
            RMLServiceRequestMessage request = null;
            try {
                String requestID = (correlationID != null ? correlationID : UUID.randomUUID().toString());
                String messageStr = new String(message.getBody(), "UTF-8");
                //log.info(messageStr);
                request = RMLService.mapper.readValue(messageStr, RMLServiceRequestMessage.class);
                
                
                RMLServiceResponseMessage response = transformer.transform(request, requestID);
                String responseStr = mapper.writeValueAsString(response);
                //log.info(responseStr);
                if(response != null) {
                    log.log(Level.INFO, "Response status:" + response.getPayload().getStatus());
                    
                }
                else {
                    throw new Exception("Transformer returned a null response");
                }
                Provenance respProv = null;
                if(request.getProvenance() != null && request.getProvenance().getContext() != null) {
                    respProv = new Provenance();
                    respProv.setContext(request.getProvenance().getContext());
                    response.setProvenance(respProv);
                }
                else {
                    log.warning("Incoming message did not contain any provenance context information. Provenance will not be recorded");
                }

                
                if (correlationID == null) {
                    log.log(Level.INFO, "Sending response without correlation ID to " + replyTo);                    
                    template.convertAndSend(replyTo, responseStr);
                } else {
                    template.convertAndSend(replyTo, (Object)responseStr, new CorrelationData(correlationID));
                }

                if(respProv != null) {
                    log.log(Level.INFO, "Sending StepExecution prov message");
                    OffsetDateTime endTime = OffsetDateTime.now();
                    String provMessageStr = RMLService.getProvenanceMessage(
                            respProv.getContext(), 
                            "success", 
                            startTime,
                            endTime,
                            request.getPayload().getRMLServiceInput().getSourceData(),
                            response.getPayload().getRMLServiceOutput().getOutput());
                    template.convertAndSend("provenance.inbox", provMessageStr);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
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
                    + "        \"status\": \"ERROR\",\n"
                    + "        \"statusMessage\": \"" + ex.getMessage() + "\"\n"                        
                    + "   }\n"
                    + "}";
                
                byte[] body = errorResponse.getBytes();
                MessageProperties props = new MessageProperties();
                if (correlationID != null) {
                    props.setCorrelationIdString(correlationID);
                }
                props.setReplyTo(replyTo);
                Message responseMessage = new Message(body, props);                
                template.send(replyTo, responseMessage);
                
                String provMessageStr = RMLService.getProvenanceMessage(
                        ctx, 
                        "ERROR", 
                        startTime,
                        OffsetDateTime.now(),
                        request.getPayload().getRMLServiceInput().getSourceData(),
                        new ArrayList<String>());
                template.convertAndSend("provenance.inbox", provMessageStr);
            }
        } catch (Exception ex) {
            // TODO: handle message sending error
            ex.printStackTrace();
        }

    }
}
