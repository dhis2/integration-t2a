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

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.AdviceWith;
import org.hisp.dhis.api.v2_37_6.model.AggregationType;
import org.hisp.dhis.api.v2_37_6.model.AnalyticsPeriodBoundary;
import org.hisp.dhis.api.v2_37_6.model.Attribute;
import org.hisp.dhis.api.v2_37_6.model.AttributeValue;
import org.hisp.dhis.api.v2_37_6.model.Attribute__1;
import org.hisp.dhis.api.v2_37_6.model.CategoryCombo;
import org.hisp.dhis.api.v2_37_6.model.DataElement;
import org.hisp.dhis.api.v2_37_6.model.DataValue__2;
import org.hisp.dhis.api.v2_37_6.model.Enrollment;
import org.hisp.dhis.api.v2_37_6.model.Event;
import org.hisp.dhis.api.v2_37_6.model.EventChart;
import org.hisp.dhis.api.v2_37_6.model.ImportSummaries;
import org.hisp.dhis.api.v2_37_6.model.OptionSet;
import org.hisp.dhis.api.v2_37_6.model.OrganisationUnit;
import org.hisp.dhis.api.v2_37_6.model.OrganisationUnitLevel;
import org.hisp.dhis.api.v2_37_6.model.Program;
import org.hisp.dhis.api.v2_37_6.model.ProgramIndicator;
import org.hisp.dhis.api.v2_37_6.model.TrackedEntityAttributeValue;
import org.hisp.dhis.api.v2_37_6.model.TrackedEntityInstance;
import org.hisp.dhis.api.v2_37_6.model.WebMessage;
import org.hisp.dhis.integration.sdk.Dhis2ClientBuilder;
import org.hisp.dhis.integration.sdk.api.Dhis2Client;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.SocketUtils;
import org.springframework.util.StreamUtils;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import io.restassured.RestAssured;

public class AbstractRouteBuilderTestCase
{
    protected static final Network NETWORK = Network.newNetwork();

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected static final PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

    protected static final GenericContainer<?> DHIS2_CONTAINER;

    @Autowired
    protected CamelContext camelContext;

    static
    {
        POSTGRESQL_CONTAINER = new PostgreSQLContainer<>(
            DockerImageName.parse( "postgis/postgis:12-3.2-alpine" ).asCompatibleSubstituteFor( "postgres" ) )
                .withDatabaseName( "dhis2" )
                .withNetworkAliases( "db" )
                .withUsername( "dhis" )
                .withPassword( "dhis" ).withNetwork( NETWORK );

        POSTGRESQL_CONTAINER.start();

        DHIS2_CONTAINER = new GenericContainer<>( "dhis2/core:2.37.6" )
            .dependsOn( POSTGRESQL_CONTAINER )
            .withClasspathResourceMapping( "dhis.conf", "/DHIS2_home/dhis.conf", BindMode.READ_WRITE )
            .withNetwork( NETWORK ).withExposedPorts( 8080 )
            .waitingFor( new HttpWaitStrategy().forStatusCode( 200 ).withStartupTimeout( Duration.ofMinutes( 2 ) ) )
            .withEnv( "WAIT_FOR_DB_CONTAINER", "db" + ":" + 5432 + " -t 0" );

        DHIS2_CONTAINER.start();
    }

    protected static Dhis2Client dhis2Client;

    protected static String t2aHttpEndpointUri;

    protected static String rootOrgUnitId;

    protected static String dataElementId;

    protected String orgUnitUnderTestId;

    @BeforeAll
    public static void beforeAll()
        throws IOException
    {

        if ( t2aHttpEndpointUri == null )
        {
            t2aHttpEndpointUri = String.format( "http://0.0.0.0:%s/dhis2/t2a", SocketUtils.findAvailableTcpPort() );
            dhis2Client = Dhis2ClientBuilder.newClient(
                "http://" + DHIS2_CONTAINER.getHost() + ":" + DHIS2_CONTAINER.getFirstMappedPort() + "/api", "admin",
                "district" ).build();
            RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

            importMetaData();
            rootOrgUnitId = createOrgUnit();
            createOrgUnitLevel();
            dataElementId = createAggregateDataElement();
            updateProgramIndicatorAttributeValue();
        }

        System.setProperty( "dhis2.api.url",
            String.format( "http://localhost:%s/api", DHIS2_CONTAINER.getFirstMappedPort() ) );
        System.setProperty( "split.periods", String.valueOf( true ) );
        System.setProperty( "thread.pool.size",
            String.valueOf( Runtime.getRuntime().availableProcessors() ) );
        System.setProperty( "http.endpoint.uri", t2aHttpEndpointUri );
    }

    @BeforeEach
    public void beforeEach()
        throws Exception
    {
        orgUnitUnderTestId = createOrgUnit( rootOrgUnitId );
        addOrgUnitToUser( orgUnitUnderTestId );
        addOrgUnitToProgram( orgUnitUnderTestId );
        createTrackedEntityInstances( orgUnitUnderTestId, 5 );

        AdviceWith.adviceWith( camelContext, "scheduleAnalyticsRoute", r -> r.weaveAddLast().to( "mock:spy" ) );
        camelContext.start();
    }

    private static void updateProgramIndicatorAttributeValue()
    {
        ProgramIndicator programIndicator = new ProgramIndicator().withName( "EIR - Births (home)" )
            .withShortName( "Births (home)" )
            .withAnalyticsPeriodBoundaries( List.of( new AnalyticsPeriodBoundary().withAnalyticsPeriodBoundaryType(
                AnalyticsPeriodBoundary.AnalyticsPeriodBoundaryType.AFTER_START_OF_REPORTING_PERIOD )
                .withBoundaryTarget( "EVENT_DATE" ).withOffsetPeriods( 0 )
                .withAdditionalProperty( "offsetPeriodType", "Quarterly" ),
                new AnalyticsPeriodBoundary().withAnalyticsPeriodBoundaryType(
                    AnalyticsPeriodBoundary.AnalyticsPeriodBoundaryType.BEFORE_END_OF_REPORTING_PERIOD )
                    .withBoundaryTarget( "EVENT_DATE" ).withOffsetPeriods( 0 )
                    .withAdditionalProperty( "offsetPeriodType", "Quarterly" ) ) )
            .withProgram( new Program().withId( "SSLpOM0r1U7" ) )
            .withAttributeValues( List.of( new AttributeValue().withValue( "BirthsHome" ).withAttribute(
                new Attribute().withId( "vudyDP7jUy5" ).withName( "Data element for aggregate data export" ) ) ) )
            .withAnalyticsType( ProgramIndicator.AnalyticsType.EVENT );

        dhis2Client.put( "programIndicators/yC212U3ifgY" ).withResource( programIndicator )
            .withParameter( "mergeMode", "MERGE" ).transfer();
    }

    private static String createAggregateDataElement()
    {
        DataElement dataElement = new DataElement().withAggregationType( AggregationType.SUM ).withCode( "BirthsHome" )
            .withDomainType( DataElement.DataElementDomain.AGGREGATE ).withValueType( OptionSet.ValueType.NUMBER )
            .withName( "EIR - Births (home)" ).withShortName( "Births (home)" )
            .withCategoryCombo( new CategoryCombo().withId( "bjDvmb4bfuf" ) );

        return (String) ((Map<String, Object>) dhis2Client.post( "dataElements" ).withResource( dataElement ).transfer()
            .returnAs( WebMessage.class ).getResponse().get()).get( "uid" );
    }

    private static void addOrgUnitToProgram( String orgUnitId )
        throws IOException
    {
        dhis2Client.post( "programs/SSLpOM0r1U7/organisationUnits/{organisationUnitId}", orgUnitId ).transfer().close();
    }

    private static void addOrgUnitToUser( String orgUnitId )
        throws IOException
    {
        dhis2Client.post( "users/M5zQapPyTZI/organisationUnits/{organisationUnitId}", orgUnitId ).transfer().close();
    }

    private static void createOrgUnitLevel()
        throws IOException
    {
        dhis2Client.post( "filledOrganisationUnitLevels" )
            .withResource( new OrganisationUnitLevel().withName( "Level 1" ).withLevel( 1 ) ).transfer().close();

        dhis2Client.post( "filledOrganisationUnitLevels" )
            .withResource( new OrganisationUnitLevel().withName( "Level 2" ).withLevel( 2 ) ).transfer().close();
    }

    private static String createOrgUnit()
    {
        return createOrgUnit( null );
    }

    private static String createOrgUnit( String parentId )
    {
        Faker faker = new Faker();
        Name name = faker.name();

        OrganisationUnit orgUnit = new OrganisationUnit().withName( name.firstName() ).withShortName( name.firstName() )
            .withOpeningDate( new Date() );

        if ( parentId != null )
        {
            orgUnit.setParent( new OrganisationUnit().withId( parentId ) );
        }

        return (String) ((Map<String, Object>) dhis2Client.post( "organisationUnits" ).withResource( orgUnit )
            .transfer().returnAs( WebMessage.class ).getResponse().get()).get( "uid" );
    }

    @AfterAll
    public static void afterAll()
    {
        System.clearProperty( "dhis2.api.url" );
        System.clearProperty( "org.unit.batch.size" );
        System.clearProperty( "split.periods" );
        System.clearProperty( "thread.pool.size" );
        System.clearProperty( "http.endpoint.uri" );
    }

    private static void importMetaData()
        throws IOException
    {
        String metaData = StreamUtils.copyToString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream( "metadata.json" ),
            Charset.defaultCharset() );

        dhis2Client.post( "metadata" ).withResource( OBJECT_MAPPER.readValue( metaData, Map.class ) )
            .withParameter( "atomicMode", "NONE" ).transfer().returnAs( WebMessage.class );
    }

    private void createTrackedEntityInstances( String orgUnitId, int amount )
        throws InterruptedException,
        ParseException
    {
        Faker faker = new Faker();
        for ( int i = 0; i < amount; i++ )
        {
            Name name = faker.name();
            String uniqueSystemIdentifier = dhis2Client.get( "trackedEntityAttributes/KSr2yTdu1AI/generate" ).transfer()
                .returnAs(
                    TrackedEntityAttributeValue.class )
                .getValue().get();

            createTrackedEntityInstance( uniqueSystemIdentifier, name, orgUnitId );
        }
        dhis2_13102();
    }

    // FIXME: https://jira.dhis2.org/browse/DHIS2-13102
    private void dhis2_13102()
        throws InterruptedException
    {
        Thread.sleep( 1000 );
    }

    protected static ImportSummaries createTrackedEntityInstance( String uniqueSystemIdentifier, Name name,
        String orgUnitId )
        throws ParseException
    {
        TrackedEntityInstance tei = new TrackedEntityInstance().withAttributes(
            List.of( new Attribute__1().withAttribute( "KSr2yTdu1AI" ).withValue( uniqueSystemIdentifier ),
                new Attribute__1().withAttribute( "NI0QRzJvQ0k" ).withValue( "2022-01-18" ),
                new Attribute__1().withAttribute( "ftFBu8mHZ4H" ).withValue( name.firstName() ),
                new Attribute__1().withAttribute( "EpbquVl5OD6" ).withValue( name.lastName() ) ) )
            .withEnrollments( List.of(
                new Enrollment().withEnrollmentDate( new SimpleDateFormat( "yyyy-MM-dd" ).parse( "2022-01-19" ) )
                    .withProgram( "SSLpOM0r1U7" ).withOrgUnit( orgUnitId )
                    .withStatus( Event.EnrollmentStatus.ACTIVE )
                    .withEvents( List.of(
                        new Event().withStatus( EventChart.EventStatus.ACTIVE ).withDueDate( "2022-01-19" )
                            .withEventDate( "2022-01-19" ).withProgramStage( "RcbCl5ww8XY" )
                            .withProgram( "SSLpOM0r1U7" ).withOrgUnit( orgUnitId ).withDataValues( List.of(
                                new DataValue__2().withDataElement( "ABhkInP0wGY" ).withValue( "HOME" )
                                    .withProvidedElsewhere( false ) ) ),
                        new Event().withStatus( EventChart.EventStatus.SCHEDULE ).withDueDate( "2022-01-19" )
                            .withProgramStage( "s53RFfXA75f" ).withProgram( "SSLpOM0r1U7" )
                            .withOrgUnit( orgUnitId ) ) ) ) )
            .withOrgUnit( orgUnitId )
            .withTrackedEntityType( "MCPQUTHX1Ze" );

        Map<String, Object> teiResponse = (Map<String, Object>) dhis2Client.post( "trackedEntityInstances" )
            .withResource( tei ).transfer().returnAs(
                WebMessage.class )
            .getResponse().get();

        return OBJECT_MAPPER.convertValue( teiResponse, ImportSummaries.class );
    }
}
