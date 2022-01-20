package org.hisp.dhis.integration.t2a.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties( ignoreUnknown = true )
public class Attribute {
    private String id;
}
