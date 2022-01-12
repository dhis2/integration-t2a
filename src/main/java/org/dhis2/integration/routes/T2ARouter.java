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
package org.dhis2.integration.routes;

import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolBuilder;
import org.apache.camel.component.properties.PropertiesComponent;
import org.dhis2.integration.model.DataValues;
import org.dhis2.integration.model.OrganisationUnits;
import org.dhis2.integration.model.ProgramIndicatorGroup;
import org.dhis2.integration.processors.DataValueSetQueryBuilder;
import org.dhis2.integration.processors.OrganisationUnitQueryBuilder;
import org.dhis2.integration.processors.ParallelDataValueSetQueryBuilder;
import org.dhis2.integration.processors.PostDataValueSetQueryBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

/**
 * Pulling program indicators one at a time as DataValueSets (beware if your
 * pool size is too big it breaks the database :-( )
 */
// @Component
public class T2ARouter extends RouteBuilder
{
    private static final String CONFIG_D2_USER = "dhis2.t2a.user";

    private static final String CONFIG_D2_PWD = "dhis2.t2a.pwd";

    private static final String CONFIG_PARALLEL = "dhis2.t2a.parallel";

    private static final String CONFIG_D2_DV_OU = "dhis2.t2a.dv.ou";

    private static final String CONFIG_D2_DV_PE = "dhis2.t2a.dv.pe";

    private static final String CONFIG_D2_DV_OUTPUT_ID_SCHEME = "dhis2.t2a.dv.outputIdScheme";

    private static final String CONFIG_D2_ANALYTICS = "dhis2.t2a.analytics";

    public static final String PROPERTY_PROGRAM_INDICATORS = "pis";

    public static final String PROPERTY_ALL_ORG_UNITS = "ous";

    public static final String PROPERTY_OU_LEVEL = "ou";

    public static final String PROPERTY_PERIOD = "pe";

    public static final String PROPERTY_OUTPUT_ID_SCHEME = "outputIdScheme";

    @Override
    public void configure()
        throws Exception
    {
        PropertiesComponent properties = (PropertiesComponent) getContext().getPropertiesComponent();

        // Pool Size
        int poolSize = Runtime.getRuntime().availableProcessors();

        Optional<String> poolSizeProperty = properties.resolveProperty( "dhis2.t2a.pool" );

        if ( poolSizeProperty.isPresent() )
        {
            poolSize = Integer.parseInt( poolSizeProperty.get() );
        }

        // parallel vs serial
        boolean parallel = Boolean.parseBoolean( properties.resolveProperty( CONFIG_PARALLEL ).orElse( "false" ) );

        // data value set args
        String ou = properties.resolveProperty( CONFIG_D2_DV_OU )
            .orElseThrow( () -> new RuntimeException( CONFIG_D2_DV_OU + " is required." ) );

        String pe = properties.resolveProperty( CONFIG_D2_DV_PE )
            .orElseThrow( () -> new RuntimeException( CONFIG_D2_DV_PE + " is required." ) );

        String outputIdScheme = properties.resolveProperty( CONFIG_D2_DV_OUTPUT_ID_SCHEME )
            .orElseThrow( () -> new RuntimeException( CONFIG_D2_DV_OUTPUT_ID_SCHEME + " is required." ) );

        String t2aAuthHeader = getAuthHeader();

        boolean runAnalytics = Boolean
            .parseBoolean( properties.resolveProperty( CONFIG_D2_ANALYTICS ).orElse( "false" ) );

        // adding global auth header and properties
        from( "timer:analytics?repeatCount=1&period=3000000" )
            .setHeader( HttpHeaders.AUTHORIZATION, constant( t2aAuthHeader ) )
            .setProperty( PROPERTY_OU_LEVEL, constant( ou ) )
            .setProperty( PROPERTY_PERIOD, constant( pe ) )
            .setProperty( PROPERTY_OUTPUT_ID_SCHEME, constant( outputIdScheme ) )
            .to( "direct:t2-auth" );

        from( "direct:t2-auth" )
            .process( new OrganisationUnitQueryBuilder() )
            .toD(
                "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/organisationUnits" )
            .unmarshal().json( OrganisationUnits.class )
            .setProperty( PROPERTY_ALL_ORG_UNITS, simple( "${body}" ) )
            .removeHeader( Exchange.HTTP_QUERY ) // removing query string, so it
                                                 // won't affect the rest of the
                                                 // route
            .to( "direct:t2-ous" );

        // start analytics task
        if ( runAnalytics )
        {
            from( "direct:t2-ous" )
                .setHeader( "CamelHttpMethod", constant( "PUT" ) )
                .log( "Scheduling analytics task" )
                .to( "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/resourceTables/analytics" )
                // todo using property instead of headers. Ask Bob!
                .setProperty( "taskId", jsonpath( "$.response.id" ) )
                .to( "direct:t2-analytics" );

            // check for the status in a loop
            from( "direct:t2-analytics" )
                .setHeader( "CamelHttpMethod", constant( "GET" ) )
                .log( "Analytics started with task ID : ${exchangeProperty.taskId}" )
                .toD(
                    "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/system/tasks/ANALYTICS_TABLE/${exchangeProperty.taskId}" )
                .setProperty( "status", jsonpath( "$[0]" ) )
                .loopDoWhile( simple( "${exchangeProperty.status[completed]} == false" ) )
                // .log( "Inside Loop : ${exchangeProperty.status[completed]}" )
                .toD(
                    "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/system/tasks/ANALYTICS_TABLE/${exchangeProperty.taskId}" )
                .setProperty( "status", jsonpath( "$[0]" ) )
                // .log( "Set property to ${exchangeProperty.status}" )
                .process( exchange -> Thread.sleep( 30000 ) )
                // .delay( 1000 )
                .end()
                .log( "Analytics Task Completed" )
                .choice()
                .when( simple( "${exchangeProperty.status[level]} == 'ERROR'" ) )
                .throwException( RuntimeException.class, "Analytics Failed : ${exchangeProperty.status[message]}" )
                .end()
                .log( "Analytics Status : ${header.status}" )
                .to( "direct:analytics-done" );
        }
        else
        {
            from( "direct:t2-ous" )
                .to( "direct:analytics-done" );
        }

        // get program indicators
        from( "direct:analytics-done" )
            .setHeader( Exchange.HTTP_METHOD, constant( "GET" ) )
            .setHeader( Exchange.HTTP_QUERY, constant(
                "fields=programIndicators[id,name,aggregateExportCategoryOptionCombo," +
                    "aggregateExportAttributeOptionCombo,attributeValues]" ) )
            .toD( "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/programIndicatorGroups/{{dhis2.t2a.pi.group}}" )
            .unmarshal().json( ProgramIndicatorGroup.class )
            .setProperty( PROPERTY_PROGRAM_INDICATORS, simple( "${body}" ) )
            .removeHeader( Exchange.HTTP_QUERY )
            .to( "direct:pis" );

        if ( parallel )
        {
            ThreadPoolBuilder builder = new ThreadPoolBuilder( getContext() );
            ExecutorService programIndicatorPool = builder.poolSize( poolSize ).maxPoolSize( 40 ).build();

            /*
             * Strategy 1: split and pull one PI at a time
             */
            from( "direct:pis" )
                .routeId( "t2a-parallel" )
                .split( method( new ProgramIndicatorSplitter(),
                    "split" ) )
                .executorService( programIndicatorPool )
                .log( "Processing programIndicator: ${body}" )
                .process( new ParallelDataValueSetQueryBuilder() )
                .setHeader( "Authorization", constant( t2aAuthHeader ) )
                .toD( "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/analytics/dataValueSet.json" )
                // .aggregate().body()
                .log( "${body}" )
                .process( new PostDataValueSetQueryBuilder() ) // should fix
                                                               // from this
                                                               // point
                // .setHeader( "Authorization", constant( t2aAuthHeader ) )
                .setHeader( "Content-Type", constant( MediaType.APPLICATION_JSON_VALUE ) )
                .log( "Posting DataValueSet" )
                .toD( "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/dataValueSets" )
                .log( "${body}" )
                .log( "T2A done" )
                .end();
        }
        else
        {
            /*
             * Strategy 2: All-in-one
             */
            from( "direct:pis" )
                .routeId( "t2a-serial" )
                .process( new DataValueSetQueryBuilder() )
                .log( "Getting the DataValueSet" )
                // .setHeader( "Authorization", constant( t2aAuthHeader ) )
                .toD( "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/analytics/dataValueSet.json" )
                .unmarshal().json( DataValues.class )
                .process( new PostDataValueSetQueryBuilder() )
                .marshal().json()
                // .setHeader( "Authorization", constant( t2aAuthHeader ) )
                .setHeader( "Content-Type", constant( MediaType.APPLICATION_JSON_VALUE ) )
                .log( "Posting DataValueSet" )
                .toD( "https://{{dhis2.t2a.host}}/{{dhis2.t2a.path}}/api/dataValueSets" )
                .log( "${body}" )
                .log( "T2A done" )
                .end();
        }
    }

    private String getAuthHeader()
    {
        PropertiesComponent properties = (PropertiesComponent) getContext().getPropertiesComponent();

        String user = properties.resolveProperty( CONFIG_D2_USER )
            .orElseThrow( () -> new RuntimeException( "User property is required." ) );
        String pwd = properties.resolveProperty( CONFIG_D2_PWD )
            .orElseThrow( () -> new RuntimeException( "pwd property is required." ) );

        return "Basic " + Base64.getEncoder().encodeToString( (user + ":" + pwd).getBytes() );
    }
}
