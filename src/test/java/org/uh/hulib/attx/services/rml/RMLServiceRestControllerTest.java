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
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import static org.mockito.Mockito.*;
import org.mockito.Matchers;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequest;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponse;

/**
 *
 * @author jkesanie
 */
@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
public class RMLServiceRestControllerTest {
    
    
    @InjectMocks
    private RMLServiceRestController instance = new RMLServiceRestController();
    
    @Mock
    private RMLIOTransformer transformer;
    
    public RMLServiceRestControllerTest() {
    }


    @Test
    public void testTransform() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String messageBody = FileUtils.readFileToString(new File(getClass().getResource("/transformURIRequest.json").toURI()));
        RMLServiceRequest request = mapper.readValue(messageBody, RMLServiceRequest.class);
        
        instance.transform(request);
        
        // should call the transformer once with the request body
        verify(transformer, times(1)).transform(eq(request), anyString());
        
        
    }
    
}
