/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uh.hulib.attx.services.rml;

import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import org.springframework.stereotype.Service;

/**
 *
 * @author jkesanie
 */

public interface Transformer {
  
    public String transformToRDF(Reader input, String configuration, String workID);
}
