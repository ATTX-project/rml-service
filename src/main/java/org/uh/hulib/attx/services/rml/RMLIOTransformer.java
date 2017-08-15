/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import java.net.URI;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 *
 * @author jkesanie
 */
@Service
public class RMLIOTransformer implements Transformer {

    private static final Logger LOG = LoggerFactory.getLogger(RMLIOTransformer.class);    
    
    @Override
    public URI transformToRDF(URI input, String configuration) {
        LOG.info("Transforming to RDF");
        LOG.info(input.toString());
        
        
        try {
            Thread.sleep(2000);
            LOG.info("Done");
            return new URI("file://output");
        } catch (Exception ex) {
            LOG.error(null, ex);
        }
        LOG.info("Error");
        return null;
    }
    
}
