/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceRequest;
import org.uh.hulib.attx.wc.uv.common.pojos.RMLServiceResponse;

/**
 *
 * @author jkesanie
 */
@RestController
public class RMLServiceRestController {

    @Autowired
    RMLIOTransformer transformer;

    @RequestMapping(
            value = "/" + RMLService.VERSION + "/transform",
            method = RequestMethod.POST,
            produces = {"application/json"},
            consumes = {"application/json"})
    public @ResponseBody
    RMLServiceResponse transform(@RequestBody RMLServiceRequest request) {
        try {
            return transformer.transform(request, UUID.randomUUID().toString());
        } catch (Exception ex) {
            Logger.getLogger(RMLServiceRestController.class.getName()).log(Level.SEVERE, null, ex);
            
            RMLServiceResponse response = new RMLServiceResponse();
            response.setProvenance(request.getProvenance());
            RMLServiceOutput responsePayload = new RMLServiceOutput();
            responsePayload.setStatus("ERROR");
            responsePayload.setStatusMessage(ex.getMessage());
            response.setPayload(responsePayload);
            return response;
        }
    }
    
/*
    @RequestMapping(
            value = "/health",
            method = RequestMethod.GET,
            produces = {"application/json"},
            consumes = {"application/json"})
    public @ResponseBody
    String health(@RequestBody RMLServiceRequest request) {    
        // check for broker connection 
        String status = "Running";
        
        return "{\"rmlservice\": \"" + status + "\"}";
    }
*/
}
