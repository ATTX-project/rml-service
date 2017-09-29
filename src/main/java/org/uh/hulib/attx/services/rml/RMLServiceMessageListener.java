/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URL;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.springframework.amqp.core.Message;
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

/**
 *
 * @author jkesanie
 */
@Component
public class RMLServiceMessageListener {

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
        String correlationID = message.getMessageProperties().getCorrelationIdString();
        String replyTo = message.getMessageProperties().getReplyTo();
        try {
            ObjectMapper mapper = new ObjectMapper();
            String messageStr = new String(message.getBody(), "UTF-8");
            //System.out.println(messageStr);
            RMLServiceRequest request = mapper.readValue(messageStr, RMLServiceRequest.class);

            System.out.println(request.getPayload().getMapping());
            System.out.println(request.getPayload().getSourceData());
            
            RMLServiceInput payload = request.getPayload();
            URL input = null;
            File tempFile = null;
            if (payload.getSourceURI() != null) {
                input = new URL(payload.getSourceURI());
            } else {
                tempFile = File.createTempFile("rmlservice", "source");
                FileUtils.write(tempFile, payload.getSourceData(), "UTF-8");
                input = tempFile.toURI().toURL();
            }
            File tempFileConfig = File.createTempFile("rmlservice", "config");
            FileUtils.write(tempFileConfig, payload.getMapping(), "UTF-8");
            File outputDir = new File("/attx-sb-shared/" + getAgentID() + "/" + (correlationID != null ? correlationID : UUID.randomUUID().toString()));

            outputDir.mkdirs(); // TODO: add error handling
            File output = new File(outputDir, "result.nt");

            transformer.transformToRDF(input, output.toURI().toURL(), tempFileConfig.toURI().toURL());

            if (tempFile != null) {
                tempFile.delete();
                tempFileConfig.delete();
            }

            RMLServiceResponse response = new RMLServiceResponse();
            RMLServiceOutput responsePayload = new RMLServiceOutput();
            responsePayload.setStatus("SUCCESS");
            responsePayload.setTransformedDatasetURL(output.toURI().toURL().toString());

            response.setPayload(responsePayload);
            
            System.out.println("Done");
            
            if(correlationID != null) {
                template.convertAndSend(replyTo, (Object)mapper.writeValueAsString(response));
            }
            else {
                template.convertAndSend(replyTo, (Object)mapper.writeValueAsString(response), new CorrelationData(correlationID));
            }
            System.out.println("Reply sent");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }


}
