/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.junit4.SpringRunner;

/**
 *
 * @author jkesanie
 */
public class RMLIOTransformerTest {
    
    RMLIOTransformer instance = new RMLIOTransformer();

    @Test   
    public void testSimpleJsonToRDF() throws Exception {
        File input = new File(getClass().getResource(
                "/etsin-org-data.json").toURI());
        File mapping = new File(getClass().getResource(
                "/etsin-org-map.ttl").toURI());
        File expOutput = new File(getClass().getResource("/etsin-org-result.nt").toURI());
        
        runTransformation(input, mapping, expOutput);
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
