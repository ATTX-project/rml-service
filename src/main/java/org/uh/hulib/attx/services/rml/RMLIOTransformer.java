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
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import java.util.logging.Logger;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.springframework.stereotype.Service;
import static org.uh.hulib.attx.services.rml.RMLService.SERVICE_NAME;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceInput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequestMessage;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponseMessage;
import org.uh.hulib.attx.wc.uv.common.pojos.Source;
import org.apache.commons.lang3.StringEscapeUtils;
import java.io.ByteArrayOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author jkesanie
 */
@Service
public class RMLIOTransformer {
    
    private static Logger log = Logger.getLogger(RMLIOTransformer.class.toString());

    public RMLServiceResponseMessage transform(RMLServiceRequestMessage request, String requestID) throws Exception {
        File tempFile = null;
        File tempFileConfig = null;
        File output = null;
        RMLServiceResponseMessage response = new RMLServiceResponseMessage();
        RMLServiceResponseMessage.RMLServiceResponsePayload responsePayload = response.new RMLServiceResponsePayload();
        RMLServiceOutput responseOutput = new RMLServiceOutput();
        //responseOutput.setContentType("application/n-triples");
        responseOutput.setContentType("application/rdf+xml");
        responseOutput.setOutput(new ArrayList<String>());
        responsePayload.setRMLServiceOutput(responseOutput);
        try {
            RMLServiceInput payload = request.getPayload().getRMLServiceInput();
            if (payload == null) {
                throw new Exception("Missing payload! ");
            }
            if (payload.getRmlMapping() == null) {
                throw new Exception("Missing mapping! ");
            }

            List<Source> sources = payload.getSourceData();
            log.info("Got sources2 :" + sources.size());
            
            int sourceIndex = 0;
            for(Source source : sources) {                
                URL input = null;
                log.info(source.getInputType());
                if ("URI".equals(source.getInputType())) {
                    input = new URL(source.getInput());
                } else {                    
                    tempFile = new File("/attx-sb-shared/rmlservice_" + System.currentTimeMillis());
                    log.info(tempFile.getAbsolutePath());
                    FileUtils.write(tempFile, source.getInput(), "UTF-8");
                    input = tempFile.toURI().toURL();
                }
                tempFileConfig = File.createTempFile("rmlservice", "config");
                FileUtils.write(tempFileConfig, payload.getRmlMapping(), "UTF-8");
                File outputDir = new File("/attx-sb-shared/" + SERVICE_NAME + "/" + requestID);

                outputDir.mkdirs(); // TODO: add error handling
                output = new File(outputDir, "result-" + sourceIndex + ".xml");
                if(!outputDir.canWrite()) {
                    throw new Exception("output file " + output.getAbsolutePath() + " cannot be written.");
                }

                transformToRDF(input, output.toURI().toURL(), tempFileConfig.toURI().toURL());               
                responseOutput.setOutputType("URI");
                responseOutput.getOutput().add("file://" + output.getAbsolutePath());
                sourceIndex++;
                
            }
            responsePayload.setStatus("success");            

        } catch (Exception ex) {
            log.log(Level.SEVERE, "Could not process payload.", ex);
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

    protected boolean transformToRDF(URL input, URL outputURL, URL configuration) throws Exception {
        
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
            ByteArrayOutputStream out=new ByteArrayOutputStream();
            /*
            output.dumpRDF(out, RDFFormat.NTRIPLES);               
            FileUtils.writeStringToFile(new File(outputURL.getFile()),StringEscapeUtils.unescapeJava(new String(out.toByteArray(), "UTF-8")));                           
           // clean it up - this is really klunky
            Pattern p = Pattern.compile("\\\"(.+)\\\" \\.");
            List<String> newLines = new ArrayList<String>();
            List<String> lines = FileUtils.readLines(new File(outputURL.getFile()));
            for (String line : lines) {
                Matcher m = p.matcher(line);
                if(m.find()) {
                    String g = m.group();
                    String g2 = g.substring(1, g.length()-3).replaceAll("\"", "'").replaceAll("\\\\", "\\\\\\\\");
                    String newLine = line.substring(0, m.start()) + "\"" + g2 + "\" .";
                    newLines.add(newLine);
                }
                else {
                    
                    newLines.add(line);
                }
                
            }
            FileUtils.writeLines(new File(outputURL.getFile()), newLines);     
            */
            output.dumpRDF(out, RDFFormat.RDFXML); 
            out.close();            
            
        } else {

            throw new Exception("Error occured");
        }
        return true;
    }
}
