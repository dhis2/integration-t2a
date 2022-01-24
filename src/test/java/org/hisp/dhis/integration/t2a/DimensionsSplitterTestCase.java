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
package org.hisp.dhis.integration.t2a;

import static org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder.SPLIT_ORG_UNITS_CONFIG;
import static org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder.SPLIT_PERIODS_CONFIG;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.engine.SimpleCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultMessage;
import org.hisp.dhis.integration.t2a.model.*;
import org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DimensionsSplitterTestCase
{
    @ParameterizedTest
    @CsvSource( { "true,true,16", "true,false,8", "false,true,4", "false,false,2" } )
    public void testSplit( String splitPeriods, String splitOrgUnits, int expectedDimensionsSize )
    {
        Properties properties = new Properties();
        properties.setProperty( SPLIT_PERIODS_CONFIG, splitPeriods );
        properties.setProperty( SPLIT_ORG_UNITS_CONFIG, splitOrgUnits );

        CamelContext camelContext = new SimpleCamelContext();
        camelContext.getPropertiesComponent().setInitialProperties( properties );

        OrganisationUnits organisationUnits = new OrganisationUnits();
        organisationUnits
            .setOrganisationUnits( Arrays.asList( new OrganisationUnit().setId( UUID.randomUUID().toString() ),
                new OrganisationUnit().setId( UUID.randomUUID().toString() ) ) );

        ProgramIndicatorGroup programIndicatorGroup = new ProgramIndicatorGroup();
        programIndicatorGroup.setProgramIndicators(
            List.of( new ProgramIndicator().setId( UUID.randomUUID().toString() ),
                new ProgramIndicator().setId( UUID.randomUUID().toString() ) ) );

        Exchange exchange = new DefaultExchange( camelContext );
        Message message = new DefaultMessage( exchange );
        message.setBody( programIndicatorGroup );

        exchange.setProperty( T2ARouteBuilder.PERIOD_PROPERTY, "2022Q1,2022Q2,2022Q3,2022Q4" );
        exchange.setMessage( message );
        exchange.setProperty( T2ARouteBuilder.ALL_ORG_UNITS_PROPERTY, organisationUnits );

        DimensionSplitter dimensionSplitter = new DimensionSplitter();
        List<Dimensions> dimensions = dimensionSplitter.split( exchange );
        assertEquals( expectedDimensionsSize, dimensions.size() );
    }
}
