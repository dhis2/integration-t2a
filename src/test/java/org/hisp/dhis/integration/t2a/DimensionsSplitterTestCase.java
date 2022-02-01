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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.engine.SimpleCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultMessage;
import org.hisp.dhis.integration.t2a.model.Dimensions;
import org.hisp.dhis.integration.t2a.model.OrganisationUnit;
import org.hisp.dhis.integration.t2a.model.OrganisationUnits;
import org.hisp.dhis.integration.t2a.model.ProgramIndicator;
import org.hisp.dhis.integration.t2a.model.ProgramIndicatorGroup;
import org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DimensionsSplitterTestCase
{
    @ParameterizedTest
    @CsvSource( { "true,1,16", "true,2,8", "false,1,4", "false,2,2" } )
    public void testSplit( boolean splitPeriods, int orgUnitBatchSize, int expectedDimensionsSize )
    {
        OrganisationUnits organisationUnits = new OrganisationUnits();
        organisationUnits
            .setOrganisationUnits( Arrays.asList( new OrganisationUnit().setId( "Bob" ),
                new OrganisationUnit().setId( "Alice" ) ) );

        ProgramIndicatorGroup programIndicatorGroup = new ProgramIndicatorGroup();
        programIndicatorGroup.setProgramIndicators(
            List.of( new ProgramIndicator().setId( UUID.randomUUID().toString() ),
                new ProgramIndicator().setId( UUID.randomUUID().toString() ) ) );

        DimensionSplitter dimensionSplitter = new DimensionSplitter();
        dimensionSplitter.setOrgUnitBatchSize( orgUnitBatchSize );
        dimensionSplitter.setPeriods( "2022Q1,2022Q2,2022Q3,2022Q4" );
        dimensionSplitter.setSplitPeriods( splitPeriods );

        List<Dimensions> dimensions = dimensionSplitter.split(
            createExchange( new SimpleCamelContext(), organisationUnits, programIndicatorGroup ) );
        assertEquals( expectedDimensionsSize, dimensions.size() );
    }

    @Test
    public void testSplitGivenSplitOrgUnitsConfigIsFalse()
    {
        OrganisationUnits organisationUnits = new OrganisationUnits();
        organisationUnits
            .setOrganisationUnits(
                Arrays.asList( new OrganisationUnit().setId( "Bob" ), new OrganisationUnit().setId( "Alice" ) ) );

        ProgramIndicatorGroup programIndicatorGroup = new ProgramIndicatorGroup();
        programIndicatorGroup.setProgramIndicators(
            List.of( new ProgramIndicator().setId( UUID.randomUUID().toString() ) ) );

        DimensionSplitter dimensionSplitter = new DimensionSplitter();
        dimensionSplitter.setOrgUnitBatchSize( 2 );
        dimensionSplitter.setPeriods( "2022Q1" );
        dimensionSplitter.setSplitPeriods( ThreadLocalRandom.current().nextBoolean() );

        List<Dimensions> dimensions = dimensionSplitter
            .split( createExchange( new SimpleCamelContext(), organisationUnits, programIndicatorGroup ) );
        assertEquals( 1, dimensions.size() );
        assertEquals( "Bob;Alice", dimensions.get( 0 ).getOrganisationUnitIds() );
        assertEquals( "2022Q1", dimensions.get( 0 ).getPeriods() );
    }

    @Test
    public void testSplitGivenSplitPeriodsConfigIsFalse()
    {
        OrganisationUnits organisationUnits = new OrganisationUnits();
        organisationUnits
            .setOrganisationUnits( List.of( new OrganisationUnit().setId( "Bob" ) ) );

        ProgramIndicatorGroup programIndicatorGroup = new ProgramIndicatorGroup();
        programIndicatorGroup.setProgramIndicators(
            List.of( new ProgramIndicator().setId( UUID.randomUUID().toString() ) ) );

        DimensionSplitter dimensionSplitter = new DimensionSplitter();
        dimensionSplitter.setOrgUnitBatchSize( ThreadLocalRandom.current().nextInt( 1, Integer.MAX_VALUE ) );
        dimensionSplitter.setPeriods( "2022Q1,2022Q2,2022Q3,2022Q4" );
        dimensionSplitter.setSplitPeriods( false );

        List<Dimensions> dimensions = dimensionSplitter.split(
            createExchange( new SimpleCamelContext(), organisationUnits, programIndicatorGroup ) );
        assertEquals( 1, dimensions.size() );
        assertEquals( "2022Q1;2022Q2;2022Q3;2022Q4", dimensions.get( 0 ).getPeriods() );
        assertEquals( "Bob", dimensions.get( 0 ).getOrganisationUnitIds() );
    }

    private Exchange createExchange( CamelContext camelContext, OrganisationUnits organisationUnits,
        ProgramIndicatorGroup programIndicatorGroup )
    {
        Exchange exchange = new DefaultExchange( camelContext );
        Message message = new DefaultMessage( exchange );
        message.setBody( programIndicatorGroup );

        exchange.setMessage( message );
        exchange.setProperty( T2ARouteBuilder.ALL_ORG_UNITS_PROPERTY, organisationUnits );

        return exchange;
    }
}
