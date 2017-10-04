/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.core.env.Environment;
import org.springframework.test.context.junit4.SpringRunner;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponse;

/**
 *
 * @author jkesanie
 */
@RunWith(MockitoJUnitRunner.class)
@SpringBootTest(properties = "agentID=test")
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

    
    
    /**
     * Test of receive method, of class RMLServiceMessageListener.
     */
    @Test
    public void testReceive() throws Exception {
        URL inputFile = new URL("file:///input.txt");        
        String correlationID = "correlationID";
        String replyTo = "replyaddress";
        
        Message message = createMessage(replyTo, correlationID);
        instance.receive(message);

        // should transform the input file 
        verify(transformer, times(1)).transformToRDF(eq(inputFile), any(URL.class), any(URL.class));
        
        // should send a reply message
        ArgumentCaptor<String> responseMessageCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<CorrelationData> correlationDataCaptor = ArgumentCaptor.forClass(CorrelationData.class);
 
        verify(template, times(1)).convertAndSend(eq(replyTo), (Object)responseMessageCaptor.capture(), correlationDataCaptor.capture());
        
        ObjectMapper mapper = new ObjectMapper();
        RMLServiceResponse response = mapper.readValue(responseMessageCaptor.getValue(), RMLServiceResponse.class);
        
        assertEquals(correlationDataCaptor.getValue().getId(), correlationID);        
        assertNotNull(response);
        assertEquals("application/n-triples", response.getPayload().getContentType());
        assertNotEquals(null, response.getPayload().getTransformedDatasetURL());
        assertTrue(response.getPayload().getTransformedDatasetURL().contains(correlationID));
        
    }

    @Test
    public void testReceiveWithoutCorrelationID() throws Exception {
        String replyTo = "replyaddress";
        Message message = createMessage(replyTo, null);
        instance.receive(message);
        
        // should send a reply message without correlationdata
        ArgumentCaptor<String> responseMessageCaptor = ArgumentCaptor.forClass(String.class); 
        verify(template, times(1)).convertAndSend(eq(replyTo), (Object)responseMessageCaptor.capture());
        
        
        RMLServiceResponse response = mapper.readValue(responseMessageCaptor.getValue(), RMLServiceResponse.class);
        
        assertNotNull(response);
        assertEquals("application/n-triples", response.getPayload().getContentType());
        assertNotEquals(null, response.getPayload().getTransformedDatasetURL());        
        
    }
    //@Test
    public void testReceiveWithMissingReplyAddress() {
        // should send the message to the error queue?
    }
    
    @Test
    public void testReceiveWithTransformationError() throws Exception {
        String statusMessage = "Error description";
        String replyTo = "test";
        doThrow(new Exception(statusMessage)).when(this.transformer).transformToRDF(any(), any(), any());
        
        Message message = createMessage(replyTo, null);
        instance.receive(message);
        
        
        
        ArgumentCaptor<String> responseMessageCaptor = ArgumentCaptor.forClass(String.class); 
        verify(template, times(1)).convertAndSend(eq(replyTo), (Object)responseMessageCaptor.capture());
        RMLServiceResponse response = mapper.readValue(responseMessageCaptor.getValue(), RMLServiceResponse.class);
      
        assertEquals(response.getPayload().getStatusMessage(), statusMessage);
        
    }
    
    @Test
    public void testReceiveWithMappingError() throws Exception {
        String replyTo = "test";      
        byte[] body = FileUtils.readFileToString(new File(getClass().getResource("/invalidTransformURIRequest.json").toURI())).getBytes();
        MessageProperties props = new MessageProperties();
        props.setReplyTo(replyTo);
        Message message = new Message(body, props);
        instance.receive(message);
        
        
        
        ArgumentCaptor<String> responseMessageCaptor = ArgumentCaptor.forClass(String.class); 
        verify(template, times(1)).convertAndSend(eq(replyTo), (Object)responseMessageCaptor.capture());
        System.out.println(responseMessageCaptor.getValue());
        RMLServiceResponse response = mapper.readValue(responseMessageCaptor.getValue(), RMLServiceResponse.class);
      
        assertTrue(response.getPayload().getStatusMessage().contains("Missing payload"));
                
    }
    
    private Message createMessage(String replyTo, String correlationID) throws Exception {
        byte[] body = FileUtils.readFileToString(new File(getClass().getResource("/transformURIRequest.json").toURI())).getBytes();
        MessageProperties props = new MessageProperties();
        if(correlationID != null) {
            props.setCorrelationIdString(correlationID);
        }        
        props.setReplyTo(replyTo);
        Message message = new Message(body, props);
        return message;
    }
}
