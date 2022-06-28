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
package org.hisp.dhis.integration.t2a.route;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.hisp.dhis.api.v2_37_6.model.DataValue__2;
import org.hisp.dhis.api.v2_37_6.model.Event;
import org.hisp.dhis.api.v2_37_6.model.EventChart;
import org.hisp.dhis.api.v2_37_6.model.ImportSummaries;
import org.hisp.dhis.api.v2_37_6.model.ListGrid;
import org.hisp.dhis.api.v2_37_6.model.TrackedEntityAttributeValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;

@SpringBootTest
@CamelSpringBootTest
@UseAdviceWith
public class T2ARouteBuilderTestCase extends AbstractRouteBuilderTestCase
{
    private MockEndpoint spyEndpoint;

    @BeforeEach
    public void beforeEach()
        throws Exception
    {
        System.setProperty( "org.unit.batch.size",
            String.valueOf( ThreadLocalRandom.current().nextInt( 1, 1024 ) ) );
        super.beforeEach();
        spyEndpoint = camelContext.getEndpoint( "mock:spy", MockEndpoint.class );
        spyEndpoint.reset();
    }

    @Test
    @DirtiesContext
    @Timeout( 180 )
    public void testHttpPostCreatesAggregateDataValue()
        throws Exception
    {
        spyEndpoint.setExpectedCount( 2 );

        given().baseUri( t2aHttpEndpointUri ).when().post().then()
            .statusCode( 204 );

        spyEndpoint.await();

        assertEquals( "5.0", dhis2Client.get( "analytics" )
            .withParameter( "dimension", String.format( "dx:%s,pe:2022Q1", dataElementId ) )
            .withParameter( "filter", String.format( "ou:%s", orgUnitUnderTestId ) )
            .withParameter( "displayProperty", "NAME" ).withParameter( "includeNumDen", "false" )
            .withParameter( "skipMeta", "true" ).withParameter( "skipData", "false" ).transfer()
            .returnAs( ListGrid.class )
            .getRows().get().get( 0 ).get( 2 ) );
    }

    @Test
    @DirtiesContext
    @Timeout( 360 )
    public void testNullDataValueOverwritesAggregateDataValue()
        throws Exception
    {
        Faker faker = new Faker();
        Name name = faker.name();
        String uniqueSystemIdentifier = dhis2Client.get( "trackedEntityAttributes/KSr2yTdu1AI/generate" ).transfer()
            .returnAs(
                TrackedEntityAttributeValue.class )
            .getValue().get();

        ImportSummaries importSummaries = createTrackedEntityInstance( uniqueSystemIdentifier, name,
            orgUnitUnderTestId );
        String trackedEntityInstanceId = importSummaries.getImportSummaries().get().get( 0 ).getReference().get();
        String eventId = importSummaries.getImportSummaries().get().get( 0 )
            .getEnrollments().get().getImportSummaries().get().get( 0 ).getEvents().get().getImportSummaries().get()
            .get( 0 ).getReference().get();

        spyEndpoint.setExpectedCount( 2 );

        given().baseUri( t2aHttpEndpointUri ).when().post().then()
            .statusCode( 204 );

        spyEndpoint.await();
        assertEquals( "6.0", dhis2Client.get( "analytics" )
            .withParameter( "dimension", String.format( "dx:%s,pe:2022Q1", dataElementId ) )
            .withParameter( "filter", String.format( "ou:%s", orgUnitUnderTestId ) )
            .withParameter( "displayProperty", "NAME" ).withParameter( "includeNumDen", "false" )
            .withParameter( "skipMeta", "true" ).withParameter( "skipData", "false" ).transfer()
            .returnAs( ListGrid.class )
            .getRows().get().get( 0 ).get( 2 ) );

        Event event = new Event().withEvent( eventId ).withOrgUnit( orgUnitUnderTestId ).withProgram( "SSLpOM0r1U7" )
            .withProgramStage( "RcbCl5ww8XY" ).withStatus(
                EventChart.EventStatus.ACTIVE )
            .withTrackedEntityInstance( trackedEntityInstanceId )
            .withDataValues(
                List.of( new DataValue__2().withDataElement( "ABhkInP0wGY" ).withProvidedElsewhere( false ) ) );
        dhis2Client.put( "events/{eventId}/ABhkInP0wGY", eventId ).withResource( event ).transfer().close();

        spyEndpoint.reset();
        spyEndpoint.setExpectedCount( 2 );
        given().baseUri( t2aHttpEndpointUri ).when().post().then()
            .statusCode( 204 );

        spyEndpoint.await();

        assertEquals( "5.0", dhis2Client.get( "analytics" )
            .withParameter( "dimension", String.format( "dx:%s,pe:2022Q1", dataElementId ) )
            .withParameter( "filter", String.format( "ou:%s", orgUnitUnderTestId ) )
            .withParameter( "displayProperty", "NAME" ).withParameter( "includeNumDen", "false" )
            .withParameter( "skipMeta", "true" ).withParameter( "skipData", "false" ).transfer()
            .returnAs( ListGrid.class )
            .getRows().get().get( 0 ).get( 2 ) );
    }
}
