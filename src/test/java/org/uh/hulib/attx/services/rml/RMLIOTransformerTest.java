/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.junit4.SpringRunner;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceInput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequest;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponse;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Context;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Provenance;

/**
 *
 * @author jkesanie
 */
public class RMLIOTransformerTest {
    
    
    RMLIOTransformer instance = new RMLIOTransformer();
    
    private ObjectMapper mapper = new ObjectMapper();
    
    private Provenance getProvenance() {
        Provenance prov = new Provenance();
        Context ctx = new Context();
        ctx.setWorkflowID("workflow");
        ctx.setActivityID("activity");
        ctx.setStepID("step");
        prov.setContext(ctx);
        return prov;
    }
    
    @Test   
    public void testTransformToRDF() throws Exception {
        File input = new File(getClass().getResource(
                "/etsin-org-data.json").toURI());
        File mapping = new File(getClass().getResource(
                "/etsin-org-map.ttl").toURI());
        File expOutput = new File(getClass().getResource("/etsin-org-result.nt").toURI());

        // basic run
        runTransformation(input, mapping, expOutput);
        
        // TODO: invalid mapping raises an error

    }
    
    /**
     * Test of receive method, of class RMLServiceMessageListener.
     */
    @Test
    public void testTransform() throws Exception {
        RMLIOTransformer instance = mock(RMLIOTransformer.class);
        when(instance.transformToRDF(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(instance.transform(any(), any())).thenCallRealMethod();
        
        // null payload throws an error
        RMLServiceRequest request = new RMLServiceRequest();
        request.setProvenance(getProvenance());        
        
        RMLServiceResponse response = instance.transform(request, "requestID");        
        assertEquals("ERROR", response.getPayload().getStatus());
        assertTrue(response.getPayload().getStatusMessage().contains("Missing payload"));
        
        // missing mapping throws an error
        RMLServiceInput requestPayload = new RMLServiceInput();
        request.setPayload(requestPayload);

        response = instance.transform(request, "requestID");        
        assertEquals("ERROR", response.getPayload().getStatus());
        assertTrue(response.getPayload().getStatusMessage().contains("Missing mapping"));
                
        // output is correctly set 
        requestPayload.setMapping("mapping");
        response = instance.transform(request, "requestID");        
        
 
        assertNotNull(response);
        assertEquals("application/n-triples", response.getPayload().getContentType());
        assertNotEquals(null, response.getPayload().getTransformedDatasetURL());
        assertTrue(response.getPayload().getTransformedDatasetURL().contains("requestID"));

    }    
    
    private void runTransformation(File input, File mappings, File expOutput)  {
        File output = null;
        try {
            output =  File.createTempFile("test", "output");
            instance.transformToRDF(input.toURI().toURL(), output.toURI().toURL(), mappings.toURI().toURL());
            
            List<String> content = FileUtils.readLines(output, "UTF-8");
            Collections.sort(content);
            List<String> expectedContent = FileUtils.readLines(expOutput, "UTF-8");
            Collections.sort(expectedContent);
            assertEquals(expectedContent, content );

        }catch(Exception ex) {
            fail(ex.getMessage());
        }finally {
            if(output != null) {
                output.delete();
            }
        }
        
        
    }

}
