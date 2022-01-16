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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.hisp.dhis.integration.t2a.model.*;
import org.hisp.dhis.integration.t2a.routes.T2ARouter;

public class PostDataValueSetQueryBuilder implements Processor
{
    public void process( Exchange exchange )
    {
        // generating all possibilities
        // todo add period loop
        // todo this could consume a lot of memory, do a streaming impl

        OrganisationUnits ous = exchange.getProperty( T2ARouter.PROPERTY_ALL_ORG_UNITS, OrganisationUnits.class );
        ProgramIndicatorGroup pis = exchange.getProperty( T2ARouter.PROPERTY_PROGRAM_INDICATORS,
            ProgramIndicatorGroup.class );

        Set<DataValue> allPossibleDataValues = new HashSet<>();

        for ( OrganisationUnit ou : ous.getOrganisationUnits() )
        {
            for ( ProgramIndicator programIndicator : pis.getProgramIndicators() )
            {
                // is loop necessary?
                for ( AttributeValue attributeValue : programIndicator.getAttributeValues() )
                {
                    DataValue dv = new DataValue();
                    dv.setValue( "0" );
                    dv.setOrgUnit( ou.getId() );
                    dv.setPeriod( "2020Q4" );
                    dv.setDataElement( attributeValue.getValue() );
                    dv.setCategoryOptionCombo( programIndicator.getAggregateExportCategoryOptionCombo() );
                    allPossibleDataValues.add( dv );
                }
            }
        }

        // now replace them with the values in hand
        DataValues dataValuesFromAnalytics = exchange.getMessage().getBody( DataValues.class );
        allPossibleDataValues.addAll( dataValuesFromAnalytics.getDataValues() );

        String query = "dataElementIdScheme=CODE" +
            "&categoryOptionComboIdScheme=CODE" +
            "&importStrategy=CREATE_AND_UPDATE" +
            "&dryRun=false";

        exchange.getMessage().setHeader( Exchange.HTTP_QUERY, query );
        exchange.getMessage().setHeader( Exchange.HTTP_METHOD, "POST" );

        // creating the final data values
        DataValues finalDataValues = new DataValues();
        // can we avoid copying?
        finalDataValues.setDataValues( new ArrayList<>( allPossibleDataValues ) );

        exchange.getMessage().setBody( finalDataValues );
    }
}
