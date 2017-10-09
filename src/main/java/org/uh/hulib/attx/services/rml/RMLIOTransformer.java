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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.springframework.stereotype.Service;
import static org.uh.hulib.attx.services.rml.RMLService.SERVICE_NAME;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceInput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequest;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponse;

/**
 *
 * @author jkesanie
 */
@Service
public class RMLIOTransformer {

    public RMLServiceResponse transform(RMLServiceRequest request, String requestID) throws Exception {
        File tempFile = null;
        File tempFileConfig = null;
        File output = null;
        RMLServiceResponse response = new RMLServiceResponse();
        RMLServiceOutput responsePayload = new RMLServiceOutput();
        responsePayload.setContentType("application/n-triples");
        
        try {
            RMLServiceInput payload = request.getPayload();
            if (payload == null) {
                throw new Exception("Missing payload! ");
            }
            if (payload.getMapping() == null) {
                throw new Exception("Missing mapping! ");
            }
            
            URL input = null;

            if (payload.getSourceURI() != null) {
                input = new URL(payload.getSourceURI());
            } else {
                tempFile = File.createTempFile("rmlservice", "source");
                FileUtils.write(tempFile, payload.getSourceData(), "UTF-8");
                input = tempFile.toURI().toURL();
            }
            tempFileConfig = File.createTempFile("rmlservice", "config");
            FileUtils.write(tempFileConfig, payload.getMapping(), "UTF-8");
            File outputDir = new File("/attx-sb-shared/" + SERVICE_NAME + "/" + requestID);

            outputDir.mkdirs(); // TODO: add error handling
            output = new File(outputDir, "result.nt");

            transformToRDF(input, output.toURI().toURL(), tempFileConfig.toURI().toURL());
            responsePayload.setStatus("SUCCESS");
            responsePayload.setTransformedDatasetURL("file://" + output.getAbsolutePath());

        } catch (Exception ex) {
            responsePayload.setStatus("ERROR");
            responsePayload.setStatusMessage(ex.getMessage());

        }
        if (tempFile != null) {
            tempFile.delete();
            tempFileConfig.delete();
        }

        response.setPayload(responsePayload);

        return response;
    }

    protected boolean transformToRDF(URL input, URL outputFile, URL configuration) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("filename", input.getFile());

        StdRMLMappingFactory mappingFactory = new StdRMLMappingFactory();
        RMLDocRetrieval mapDocRetrieval = new RMLDocRetrieval();
        Repository repository = mapDocRetrieval.getMappingDoc(
                configuration.getFile(), RDFFormat.TURTLE);

        RMLEngine engine = new StdRMLEngine();
        RMLMapping mapping = mappingFactory.extractRMLMapping(repository);
        RMLDataset output = engine.chooseSesameDataSet("dataset", null, null);
        output = engine.runRMLMapping(output,
                mapping, "http://example.com", parameters, null);

        if (output != null) {
            FileOutputStream out = new FileOutputStream(outputFile.getFile());
            output.dumpRDF(out, RDFFormat.NTRIPLES);

        } else {

            throw new Exception("Error occured");
        }
        return true;
    }
}
