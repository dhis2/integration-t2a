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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
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

@SpringBootTest
@CamelSpringBootTest
@Testcontainers
@UseAdviceWith
public class T2ARouteBuilderTest
{
    public static Network network = Network.newNetwork();

    private static String dataElementId;

    private static String orgUnitId;

    @Autowired
    private CamelContext camelContext;

    @Container
    public static PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>(
        DockerImageName.parse( "postgis/postgis:12-3.2-alpine" ).asCompatibleSubstituteFor( "postgres" ) )
            .withDatabaseName( "dhis2" )
            .withNetworkAliases( "db" )
            .withUsername( "dhis" )
            .withPassword( "dhis" ).withNetwork( network );

    @Container
    public static GenericContainer<?> dhis2Container = new GenericContainer<>( "dhis2/core:2.36.0" )
        .dependsOn( postgreSQLContainer )
        .withClasspathResourceMapping( "dhis.conf", "/DHIS2_home/dhis.conf", BindMode.READ_WRITE )
        .withNetwork( network ).withExposedPorts( 8080 ).waitingFor( new HttpWaitStrategy().forStatusCode( 200 ) )
        .withEnv( "WAIT_FOR_DB_CONTAINER", "db" + ":" + 5432 + " -t 0" );

    @BeforeAll
    public static void beforeAll()
        throws IOException
    {
        System.setProperty( "dhis2.api.url",
            String.format( "http://localhost:%s/api", dhis2Container.getFirstMappedPort() ) );
        RestAssured.baseURI = "http://" + dhis2Container.getHost() + ":" + dhis2Container.getFirstMappedPort();
        RestAssured.requestSpecification = new RequestSpecBuilder().build().contentType( ContentType.JSON ).header(
            HttpHeaders.AUTHORIZATION,
            "Basic " + Base64.getEncoder().encodeToString( ("admin" + ":" + "district").getBytes() ) );
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

    private static void updateProgramIndicatorAttributeValue()
        throws IOException
    {
        String programIndicator = StreamUtils.copyToString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream( "programIndicator.json" ),
            Charset.defaultCharset() );

        given().queryParam( "mergeMode", "MERGE" ).body( programIndicator ).when()
            .put( "api/programIndicators/BeNeZroDY95" ).then().statusCode( 200 );
    }

    private static String createAggregateDataElement()
    {
        return given().body( new HashMap<>()
        {
            {
                put( "aggregationType", "SUM" );
                put( "code", "InfantsWithVisitUnder1Year" );
                put( "domainType", "AGGREGATE" );
                put( "valueType", "NUMBER" );
                put( "name", "Infants with visit under 1 year" );
                put( "shortName", "Infants with visit under 1 year" );
                put( "categoryCombo", new HashMap<>()
                {
                    {
                        put( "id", "bjDvmb4bfuf" );
                    }
                } );
                put( "aggregationLevels", List.of( 1 ) );
            }
        } ).when().post( "api/dataElements" ).then().statusCode( 201 ).extract().body().path( "response.uid" );
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
        given().body( new HashMap<>()
        {
            {
                put( "organisationUnitLevels", List.of( new HashMap<>()
                {
                    {
                        put( "name", "Level 1" );
                        put( "level", 1 );
                    }
                } ) );
            }
        } ).when().post( "api/filledOrganisationUnitLevels" ).then().statusCode( 201 );
    }

    private static String createOrgUnit()
    {
        return given().body( new HashMap<>()
        {
            {
                put( "name", "Acme" );
                put( "shortName", "Acme" );
                put( "openingDate", new Date() );
            }
        } ).when().post( "api/organisationUnits" ).then().statusCode( 201 ).extract().path( "response.uid" );
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
        String trackedEntityInstance = StreamUtils.copyToString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream( "trackedEntityInstance.json" ),
            Charset.defaultCharset() );

        Faker faker = new Faker();
        for ( int i = 0; i < 5; i++ )
        {
            Name name = faker.name();
            String uniqueSystemIdentifier = given().get( "api/trackedEntityAttributes/KSr2yTdu1AI/generate" ).then()
                .statusCode( 200 ).extract().path( "value" );

            given().body( String.format( trackedEntityInstance, orgUnitId, uniqueSystemIdentifier, name.firstName(),
                name.lastName() ) ).when().post( "api/trackedEntityInstances" ).then().statusCode( 200 );
        }
    }

    @Test
    @Timeout( 180 )
    public void test()
        throws Exception
    {
        MockEndpoint spyEndpoint = camelContext.getEndpoint( "mock:spy", MockEndpoint.class );
        spyEndpoint.setExpectedCount( 2 );
        AdviceWith.adviceWith( camelContext, "analyticsRoute", r -> r.weaveAddLast().to( "mock:spy" ) );

        camelContext.start();

        spyEndpoint.await();
        when().get( "api/dataValues?de={dataElement}&pe=2022Q1&ou={organisationUnit}", dataElementId, orgUnitId ).then()
            .statusCode( 200 );
    }
}
