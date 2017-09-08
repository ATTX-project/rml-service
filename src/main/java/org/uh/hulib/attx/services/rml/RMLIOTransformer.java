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
import java.io.FileOutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.springframework.stereotype.Service;

/**
 *
 * @author jkesanie
 */
@Service
public class RMLIOTransformer {
   
    public void transformToRDF(URL input, URL outputFile, URL configuration) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("filename",input.getFile());                

        StdRMLMappingFactory mappingFactory = new StdRMLMappingFactory();
        RMLDocRetrieval mapDocRetrieval = new RMLDocRetrieval();
        Repository repository = mapDocRetrieval.getMappingDoc(
            configuration.getFile(), RDFFormat.TURTLE);

        RMLEngine engine = new StdRMLEngine();
        RMLMapping mapping = mappingFactory.extractRMLMapping(repository);
        RMLDataset output = engine.chooseSesameDataSet("dataset", null, null);
        output = engine.runRMLMapping(output,
            mapping, "http://example.com", parameters, null);

        if(output != null) {
            FileOutputStream out = new FileOutputStream(outputFile.getFile());
            output.dumpRDF(out, RDFFormat.NTRIPLES);


        }
        else {


            throw new Exception("Error occured");
        }
        
    }    
}
