/*
 * Copyright (c) 2004-2022, University of Oslo
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * Neither the name of the HISP project nor the names of its contributors may
 * be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.hisp.dhis.integration.t2a.processors;

import java.util.*;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.hisp.dhis.integration.t2a.model.*;
import org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AnalyticsGridToDataValueSetQueryBuilder implements Processor
{
    /**
     * Using Jackson Mapper is a temporary workaround since
     * exchange.getMessage().getBody( AnalyticsGrid.class ); doesn't work.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    public void process( Exchange exchange )
        throws JsonProcessingException
    {

        ArrayList<DataValue> dataValues = new ArrayList<>();

        String analyticsGridStr = exchange.getMessage().getBody( String.class );
        AnalyticsGrid analyticsGrid = mapper.readValue( analyticsGridStr, AnalyticsGrid.class );

        Dimensions dimensions = exchange.getProperty( T2ARouteBuilder.DIMENSIONS_PROPERTY,
            Dimensions.class );

        String query = "dataElementIdScheme=CODE" +
            "&categoryOptionComboIdScheme=CODE" +
            "&importStrategy=CREATE_AND_UPDATE" +
            "&dryRun=false";

        exchange.getMessage().setHeader( Exchange.HTTP_QUERY, query );
        exchange.getMessage().setHeader( Exchange.HTTP_METHOD, "POST" );

        Optional<AttributeValue> aggregateDataExportDataElementOptional = dimensions.getProgramIndicator()
            .getAttributeValues().stream().filter( av -> av.getAttribute().getId().equals( "vudyDP7jUy5" ) )
            .findFirst();

        if ( aggregateDataExportDataElementOptional.isPresent() )
        {
            for ( List<String> row : analyticsGrid.getRows() )
            {
                String ou = row.get( 0 );
                String pe = row.get( 4 );
                String value = row.get( 8 );

                DataValue dv = new DataValue();
                dv.setValue( StringUtils.hasText( value ) ? value : "0" );
                dv.setOrgUnit( ou );
                dv.setPeriod( pe );
                dv.setDataElement( aggregateDataExportDataElementOptional.get().getValue() );
                dv.setCategoryOptionCombo( dimensions.getProgramIndicator().getAggregateExportCategoryOptionCombo() );

                dataValues.add( dv );
            }
        }

        DataValues finalDataValues = new DataValues();
        finalDataValues.setDataValues( dataValues );

        // todo just setting the POJO doesn't work. Should investigate
        // leisurely.
        exchange.getMessage().setBody( mapper.writeValueAsString( finalDataValues ) );
    }
}
