/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import static org.uh.hulib.attx.services.rml.RMLService.defaultAgentID;

/**
 *
 * @author jkesanie
 */
@Service
public class RMLIOTransformer implements Transformer {

    private static final Logger LOG = LoggerFactory.getLogger(RMLIOTransformer.class);    
    
    @Autowired
    private Environment env;
    
    public String getAgentID() {
        return env.getProperty("agentID", defaultAgentID);
    }
    
    @Override
    public String transformToRDF(Reader input, String configuration, String workID) {
        LOG.info("Transforming to RDF");
       
        
        
        try {
            LOG.info("Input is ready: " + input.ready());
            File outputDir = new File("/attx-sb-shared/" + getAgentID() + "/" + workID);
            if(outputDir.mkdirs()) {
                File outputFile = new File(outputDir, "result.nt");


                FileWriter writer = new FileWriter(outputFile);
                writer.write("<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> .");
                Thread.sleep(2000);
                LOG.info("Done");
                return outputFile.toURI().toString();
                
            }
        } catch (Exception ex) {
            LOG.error(null, ex);
        }
        LOG.info("Error");
        return null;
    }
    
}
