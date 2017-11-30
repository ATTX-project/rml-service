/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponseMessage;
  
/**
 *
 * @author jkesanie
 */
@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
public class RMLServiceMessageListenerTest {

    @InjectMocks
    private RMLServiceMessageListener instance = new RMLServiceMessageListener();

    @Mock
    private Environment env;

    @Mock
    private Queue queue;

    @Mock
    private RabbitTemplate template;

    @Mock
    private RMLIOTransformer transformer;

    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void init() {
        when(this.env.getProperty("agentID", "rmlservice")).thenReturn("rmlservice");
        when(this.queue.getName()).thenReturn("rmlservice");
        
        RMLServiceResponseMessage response = new RMLServiceResponseMessage();
        RMLServiceResponseMessage.RMLServiceResponsePayload payload = response.new RMLServiceResponsePayload();
        RMLServiceOutput output = new RMLServiceOutput();
        payload.setRMLServiceOutput(output);
        response.setPayload(payload);
        response.getPayload().getRMLServiceOutput().setContentType("application/json");
        response.getPayload().setStatus("success");
        response.getPayload().setStatusMessage("");
        response.getPayload().getRMLServiceOutput().setOutput(new ArrayList<String>());
        response.getPayload().getRMLServiceOutput().getOutput().add("file:///temp/file.nt");
        
        try {
            when(this.transformer.transform(any(), any())).thenReturn(response);
        } catch (Exception ex) {
            Logger.getLogger(RMLServiceMessageListenerTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Test of getAgentID method, of class RMLServiceMessageListener.
     */
    @Test
    public void testGetAgentID() {

        System.out.println("getAgentID");
        String expResult = "rmlservice";
        String result = instance.getAgentID();
        assertEquals(expResult, result);
    }

    /**
     * Test of getQueueName method, of class RMLServiceMessageListener.
     */
    @Test
    public void testGetQueueName() {
        System.out.println("getQueueName");
        String expResult = "rmlservice";
        String result = instance.getQueueName();
        assertEquals(expResult, result);
    }
    
    @Test
    public void testReceive() throws Exception {
        String correlationID = "correlationID";
        String replyTo = "replyaddress";
        Message message = createMessage(replyTo, correlationID);
        instance.receive(message);

        // should transform the input file 
        verify(transformer, times(1)).transform(any(), eq(correlationID));

        // should send a reply message
        ArgumentCaptor<String> responseMessageCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<CorrelationData> correlationDataCaptor = ArgumentCaptor.forClass(CorrelationData.class);

        verify(template, times(1)).convertAndSend(eq(replyTo), (Object) responseMessageCaptor.capture(), correlationDataCaptor.capture());        

        // should send a provenance message
        verify(template, times(1)).convertAndSend(eq("provenance.inbox"), (String)any());
    }    
    
    @Test
    public void testReceiveWithError() throws Exception {
        String statusMessage = "Error description";        
        doThrow(new Exception(statusMessage)).when(this.transformer).transform(any(), any());
        
        String correlationID = null;
        String replyTo = "replyaddress";
        Message message = createMessage(replyTo, correlationID);
        instance.receive(message);

        ArgumentCaptor<Message> responseMessageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(template, times(1)).send(eq(replyTo), responseMessageCaptor.capture());
        Message responseMsg = responseMessageCaptor.getValue();
        RMLServiceResponseMessage response = mapper.readValue(responseMsg.getBody(), RMLServiceResponseMessage.class);
        assertEquals(response.getPayload().getStatusMessage(), statusMessage);        
        
        // should send a provenance message
        verify(template, times(1)).convertAndSend(eq("provenance.inbox"), (String)any());
        
    }


    @Test
    public void testReceiveWithoutCorrelationID() throws Exception {
        String replyTo = "replyaddress";
        Message message = createMessage(replyTo, null);
        instance.receive(message);

        // should send a reply message without correlationdata
        ArgumentCaptor<String> responseMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(template, times(1)).convertAndSend(eq(replyTo), (Object) responseMessageCaptor.capture());
        
        // should send a provenance message
        verify(template, times(1)).convertAndSend(eq("provenance.inbox"), (String)any());
        

    }

    //@Test
    public void testReceiveWithMissingReplyAddress() {
        // should send the message to the error queue?
    }

    @Test
    public void testReceiveWithTransformationError() throws Exception {
        String statusMessage = "Error description";
        String replyTo = "test";
        doThrow(new Exception(statusMessage)).when(this.transformer).transform(any(), any());

        Message message = createMessage(replyTo, null);
        instance.receive(message);

        ArgumentCaptor<Message> responseMessageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(template, times(1)).send(eq(replyTo), responseMessageCaptor.capture());
        
        Object o = responseMessageCaptor.getValue();
        RMLServiceResponseMessage response = mapper.readValue(new String(responseMessageCaptor.getValue().getBody(), "UTF-8"), RMLServiceResponseMessage.class);

        assertEquals(response.getPayload().getStatusMessage(), statusMessage);

        // should send a provenance message
        verify(template, times(1)).convertAndSend(eq("provenance.inbox"), (String)any());
        
    }

    private Message createMessage(String replyTo, String correlationID) throws Exception {
        byte[] body = FileUtils.readFileToString(new File(getClass().getResource("/transformURIRequest.json").toURI())).getBytes();
        MessageProperties props = new MessageProperties();
        if (correlationID != null) {
            props.setCorrelationIdString(correlationID);
        }
        props.setReplyTo(replyTo);
        Message message = new Message(body, props);
        return message;
    }
}
