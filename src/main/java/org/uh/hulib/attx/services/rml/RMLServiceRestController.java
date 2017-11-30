/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequestMessage;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponseMessage;

/**
 *
 * @author jkesanie
 */
@RestController
public class RMLServiceRestController {
    private static Logger log = Logger.getLogger(RMLServiceRestController.class.toString());
    @Autowired
    RMLIOTransformer transformer;
    
    @Autowired
    RabbitTemplate template;    

    @RequestMapping(
            value = "/" + RMLService.VERSION + "/transform",
            method = RequestMethod.POST,
            produces = {"application/json"},
            consumes = {"application/json"})
    public @ResponseBody
    RMLServiceResponseMessage transform(@RequestBody RMLServiceRequestMessage request) {
        try {
            OffsetDateTime startTime = OffsetDateTime.now();
            RMLServiceResponseMessage response = transformer.transform(request, UUID.randomUUID().toString());
            OffsetDateTime endTime = OffsetDateTime.now();
            if(request.getProvenance() != null && request.getProvenance().getContext() != null) {
                log.log(Level.INFO, "Sending StepExecution prov message");
                String provMessageStr = RMLService.getProvenanceMessage(
                        request.getProvenance().getContext(), 
                        "success", 
                        startTime,                                                
                        endTime,
                        request.getPayload().getRMLServiceInput().getSourceData(),
                        response.getPayload().getRMLServiceOutput().getOutput());
                template.convertAndSend("provenance.inbox", provMessageStr);
            }
            else {
                log.info("No provenance context found in the request");
            }
            return response;
        } catch (Exception ex) {
            Logger.getLogger(RMLServiceRestController.class.getName()).log(Level.SEVERE, null, ex);
            
            RMLServiceResponseMessage response = new RMLServiceResponseMessage();
            response.setProvenance(request.getProvenance());
            RMLServiceResponseMessage.RMLServiceResponsePayload payload = response.new RMLServiceResponsePayload();
            payload.setStatus("ERROR");
            payload.setStatusMessage(ex.getMessage());
            response.setPayload(payload);
            return response;
        }
    }
    
}
