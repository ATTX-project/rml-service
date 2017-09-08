/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import be.ugent.mmlab.rml.core.RMLEngine;
import be.ugent.mmlab.rml.core.StdRMLEngine;
import be.ugent.mmlab.rml.mapdochandler.extraction.std.StdRMLMappingFactory;
import be.ugent.mmlab.rml.mapdochandler.retrieval.RMLDocRetrieval;
import be.ugent.mmlab.rml.model.RMLMapping;
import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
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

            RMLServiceRequest request = mapper.readValue(new String(message.getBody(), "UTF-8"), RMLServiceRequest.class);

            System.out.println(request.getMapping());
            System.out.println(request.getSourceData());
            URL input = null;
            File tempFile = null;
            if (request.getSourceURI() != null) {
                input = new URL(request.getSourceURI());
            } else {
                tempFile = File.createTempFile("rmlservice", "source");
                FileUtils.write(tempFile, request.getSourceData(), "UTF-8");
                input = tempFile.toURI().toURL();
            }
            File tempFileConfig = File.createTempFile("rmlservice", "config");
            FileUtils.write(tempFileConfig, request.getMapping(), "UTF-8");
            File outputDir = new File("attx-sb-shared/" + getAgentID() + "/" + (correlationID != null ? correlationID : UUID.randomUUID().toString()));

            outputDir.mkdirs(); // TODO: add error handling
            File output = new File(outputDir, "result.nt");

            transformer.transformToRDF(input, output.toURI().toURL(), tempFileConfig.toURI().toURL());

            if (tempFile != null) {
                tempFile.delete();
                tempFileConfig.delete();
            }

            RMLServiceResponse response = new RMLServiceResponse();
            response.setStatus("SUCCESS");
            response.setTransformedDatasetURL(output.toURI().toURL().toString());

            System.out.println("Done");
            
            if(correlationID != null) {
                template.convertAndSend(replyTo, (Object)mapper.writeValueAsString(response));
            }
            else {
                template.convertAndSend(replyTo, (Object)mapper.writeValueAsString(response), new CorrelationData(correlationID));
            }
            output.delete();
            outputDir.delete();
            System.out.println("Reply sent");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }


}
