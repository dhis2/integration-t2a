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
package org.hisp.dhis.integration.t2a.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.hisp.dhis.api.model.v2_37_7.AttributeValue;
import org.hisp.dhis.api.model.v2_37_7.DataValueSet;
import org.hisp.dhis.api.model.v2_37_7.DataValue__1;
import org.hisp.dhis.api.model.v2_37_7.ListGrid;
import org.hisp.dhis.integration.t2a.model.Dimensions;
import org.hisp.dhis.integration.t2a.route.T2ARouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class AnalyticsGridToDataValueSetQueryBuilder implements Processor
{
    @Value( "${aggr.data.export.de.id:vudyDP7jUy5}" )
    private String aggrDataExportAttrId;

    public void process( Exchange exchange )
    {
        DataValueSet dataValueSet = new DataValueSet();
        dataValueSet.setDataValues( new ArrayList<>() );

        ListGrid listGrid = exchange.getMessage().getBody( ListGrid.class );
        Dimensions dimensions = exchange.getProperty( T2ARouteBuilder.DIMENSIONS_PROPERTY,
            Dimensions.class );

        exchange.getMessage().setHeader( "CamelDhis2.queryParams",
            Map.of( "dataElementIdScheme", List.of( "CODE" ), "categoryOptionComboIdScheme", List.of( "CODE" ),
                "importStrategy", List.of( "CREATE_AND_UPDATE" ), "dryRun", List.of( "false" ) ) );

        Optional<AttributeValue> aggregateDataExportAttrOptional = dimensions.getProgramIndicator()
            .getAttributeValues().get().stream()
            .filter( av -> av.getAttribute().get().getId().get().equals( aggrDataExportAttrId ) )
            .findFirst();

        if ( aggregateDataExportAttrOptional.isPresent() )
        {
            for ( List<Object> row : listGrid.getRows().get() )
            {
                String ou = (String) row.get( 0 );
                String pe = (String) row.get( 4 );
                String value = (String) row.get( 8 );

                DataValue__1 dv = new DataValue__1();
                dv.setValue( StringUtils.hasText( value ) ? value : "0" );
                dv.setOrgUnit( ou );
                dv.setPeriod( pe );
                dv.setDataElement( aggregateDataExportAttrOptional.get().getValue().get() );
                dv.setCategoryOptionCombo(
                    dimensions.getProgramIndicator().getAggregateExportCategoryOptionCombo().orElse( null ) );
                dv.setAttributeOptionCombo(
                    dimensions.getProgramIndicator().getAggregateExportAttributeOptionCombo().orElse( null ) );

                dataValueSet.getDataValues().get().add( dv );
            }
        }

        exchange.getMessage().setBody( dataValueSet );
    }

    public String getAggrDataExportAttrId()
    {
        return aggrDataExportAttrId;
    }

    public void setAggrDataExportAttrId( String aggrDataExportAttrId )
    {
        this.aggrDataExportAttrId = aggrDataExportAttrId;
    }
}
