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

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.SocketUtils;
import org.springframework.util.StreamUtils;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;

@SpringBootTest
@CamelSpringBootTest
@Testcontainers
@UseAdviceWith
public class T2ARouteBuilderTestCase
{
    public static final Network NETWORK = Network.newNetwork();

    private static String dataElementId;

    private static String orgUnitId;

    private static String t2aHttpEndpointUri;

    @Autowired
    private CamelContext camelContext;

    private MockEndpoint spyEndpoint;

    @Container
    public static final PostgreSQLContainer<?> POSTGRESQL_CONTAINER = new PostgreSQLContainer<>(
        DockerImageName.parse( "postgis/postgis:12-3.2-alpine" ).asCompatibleSubstituteFor( "postgres" ) )
            .withDatabaseName( "dhis2" )
            .withNetworkAliases( "db" )
            .withUsername( "dhis" )
            .withPassword( "dhis" ).withNetwork( NETWORK );

    @Container
    public static final GenericContainer<?> DHIS2_CONTAINER = new GenericContainer<>( "dhis2/core:2.36.7" )
        .dependsOn( POSTGRESQL_CONTAINER )
        .withClasspathResourceMapping( "dhis.conf", "/DHIS2_home/dhis.conf", BindMode.READ_WRITE )
        .withNetwork( NETWORK ).withExposedPorts( 8080 ).waitingFor( new HttpWaitStrategy().forStatusCode( 200 ) )
        .withEnv( "WAIT_FOR_DB_CONTAINER", "db" + ":" + 5432 + " -t 0" );

    @BeforeAll
    public static void beforeAll()
        throws IOException
    {
        System.setProperty( "dhis2.api.url",
            String.format( "http://localhost:%s/api", DHIS2_CONTAINER.getFirstMappedPort() ) );
        System.setProperty( "org.unit.batch.size",
            String.valueOf( ThreadLocalRandom.current().nextInt( 1, 1024 ) ) );
        System.setProperty( "split.periods",
            String.valueOf( ThreadLocalRandom.current().nextBoolean() ).toLowerCase() );
        System.setProperty( "thread.pool.size",
            String.valueOf( Runtime.getRuntime().availableProcessors() ) );
        t2aHttpEndpointUri = String.format( "http://0.0.0.0:%s/dhis2/t2a", SocketUtils.findAvailableTcpPort() );
        System.setProperty( "http.endpoint.uri", t2aHttpEndpointUri );

        RestAssured.baseURI = "http://" + DHIS2_CONTAINER.getHost() + ":" + DHIS2_CONTAINER.getFirstMappedPort();
        RestAssured.requestSpecification = new RequestSpecBuilder().build().contentType( ContentType.JSON ).auth()
            .preemptive()
            .basic( "admin", "district" );
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

        importMetaData();
        orgUnitId = createOrgUnit();
        createOrgUnitLevel();
        addOrgUnitToUser( orgUnitId );
        dataElementId = createAggregateDataElement();
        addOrgUnitToProgram( orgUnitId );
        updateProgramIndicatorAttributeValue();
        createTrackedEntityInstances();
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

    @BeforeEach
    public void beforeEach()
        throws Exception
    {
        if ( !camelContext.isStarted() )
        {
            AdviceWith.adviceWith( camelContext, "pollAnalyticsRoute", r -> r.weaveAddLast().to( "mock:spy" ) );
            camelContext.start();
        }

        spyEndpoint = camelContext.getEndpoint( "mock:spy", MockEndpoint.class );
        spyEndpoint.reset();
    }

    private static void updateProgramIndicatorAttributeValue()
        throws IOException
    {
        String programIndicator = StreamUtils.copyToString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream( "programIndicator.json" ),
            Charset.defaultCharset() );

        given().queryParam( "mergeMode", "MERGE" ).body( programIndicator ).when()
            .put( "api/programIndicators/yC212U3ifgY" ).then().statusCode( 200 );
    }

    private static String createAggregateDataElement()
    {
        Map<String, Object> dataElement = Map.of( "aggregationType", "SUM",
            "code", "BirthsHome",
            "domainType", "AGGREGATE",
            "valueType", "NUMBER",
            "name", "EIR - Births (home)",
            "shortName", "Births (home)",
            "categoryCombo", Map.of( "id", "bjDvmb4bfuf" ),
            "aggregationLevels", List.of( 1 ) );

        return given().body( dataElement ).when().post( "api/dataElements" ).then().statusCode( 201 ).extract().body()
            .path( "response.uid" );
    }

    private static void addOrgUnitToProgram( String orgUnitId )
    {
        when().post( "api/programs/SSLpOM0r1U7/organisationUnits/{organisationUnitId}", orgUnitId ).then()
            .statusCode( 204 );
    }

    private static void addOrgUnitToUser( String orgUnitId )
    {
        when().post( "api/users/M5zQapPyTZI/organisationUnits/{organisationUnitId}", orgUnitId ).then()
            .statusCode( 204 );
    }

    private static void createOrgUnitLevel()
    {
        Map<String, List<Map<String, ? extends Serializable>>> orgUnitLevels = Map.of(
            "organisationUnitLevels", List.of( Map.of( "name", "Level 1", "level", 1 ) ) );
        given().body( orgUnitLevels ).when().post( "api/filledOrganisationUnitLevels" ).then().statusCode( 201 );
    }

    private static String createOrgUnit()
    {
        Map<String, ? extends Serializable> orgUnit = Map.of( "name", "Acme",
            "shortName", "Acme",
            "openingDate", new Date() );
        return given().body( orgUnit ).when().post( "api/organisationUnits" ).then().statusCode( 201 ).extract()
            .path( "response.uid" );
    }

    private static void importMetaData()
        throws IOException
    {
        String metaData = StreamUtils.copyToString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream( "metadata.json" ),
            Charset.defaultCharset() );
        given().queryParam( "atomicMode", "NONE" ).body( metaData ).when().post( "api/metadata" ).then()
            .statusCode( 200 );
    }

    private static void createTrackedEntityInstances()
        throws IOException
    {
        Faker faker = new Faker();
        for ( int i = 0; i < 5; i++ )
        {
            Name name = faker.name();
            String uniqueSystemIdentifier = given().get( "api/trackedEntityAttributes/KSr2yTdu1AI/generate" ).then()
                .statusCode( 200 ).extract().path( "value" );

            createTrackedEntityInstance( uniqueSystemIdentifier, name );
        }
    }

    private static JsonPath createTrackedEntityInstance( String uniqueSystemIdentifier, Name name )
        throws IOException
    {
        String trackedEntityInstance = StreamUtils.copyToString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream( "trackedEntityInstancePost.json" ),
            Charset.defaultCharset() );

        return given().body(
            String.format( trackedEntityInstance, orgUnitId, uniqueSystemIdentifier, name.firstName(),
                name.lastName() ) )
            .when().post( "api/trackedEntityInstances" ).then().statusCode( 200 ).extract().jsonPath();
    }

    @Test
    @Timeout( 180 )
    public void testHttpPostCreatesAggregateDataValue()
        throws Exception
    {
        spyEndpoint.setExpectedCount( 2 );

        given().baseUri( t2aHttpEndpointUri ).when().post().then()
            .statusCode( 204 );

        spyEndpoint.await();

        when().get( "api/dataValues?de={dataElement}&pe=2022Q1&ou={organisationUnit}", dataElementId,
            orgUnitId ).then()
            .statusCode( 200 ).body( "[0]", equalTo( "5.0" ) );
    }

    @Test
    @Timeout( 360 )
    public void testNullDataValueOverwritesAggregateDataValue()
        throws Exception
    {
        Faker faker = new Faker();
        Name name = faker.name();
        String uniqueSystemIdentifier = given().get( "api/trackedEntityAttributes/KSr2yTdu1AI/generate" ).then()
            .statusCode( 200 ).extract().path( "value" );

        JsonPath jsonPath = createTrackedEntityInstance( uniqueSystemIdentifier, name );
        String trackedEntityInstanceId = jsonPath.get( "response.importSummaries[0].reference" );
        String eventId = jsonPath.get(
            "response.importSummaries[0].enrollments.importSummaries[0].events.importSummaries[0].reference" );

        spyEndpoint.setExpectedCount( 2 );

        given().baseUri( t2aHttpEndpointUri ).when().post().then()
            .statusCode( 204 );

        spyEndpoint.await();
        when().get( "api/dataValues?de={dataElement}&pe=2022Q1&ou={organisationUnit}", dataElementId,
            orgUnitId ).then()
            .statusCode( 200 ).body( "[0]", equalTo( "6.0" ) );

        String eventPut = StreamUtils.copyToString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream( "eventPut.json" ),
            Charset.defaultCharset() );

        given().body( String.format( eventPut, eventId, orgUnitId, trackedEntityInstanceId ) ).when()
            .put( "api/events/{eventId}/ABhkInP0wGY", eventId ).then().statusCode( 200 );

        spyEndpoint.reset();
        spyEndpoint.setExpectedCount( 2 );
        given().baseUri( t2aHttpEndpointUri ).when().post().then()
            .statusCode( 204 );

        spyEndpoint.await();

        when().get( "api/dataValues?de={dataElement}&pe=2022Q1&ou={organisationUnit}", dataElementId,
            orgUnitId ).then()
            .statusCode( 200 ).body( "[0]", equalTo( "5.0" ) );
    }
}
