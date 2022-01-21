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
package org.hisp.dhis.integration.t2a.routes;

import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolBuilder;
import org.apache.camel.component.properties.PropertiesComponent;
import org.hisp.dhis.integration.t2a.DimensionSplitter;
import org.hisp.dhis.integration.t2a.model.OrganisationUnits;
import org.hisp.dhis.integration.t2a.model.ProgramIndicatorGroup;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridQueryBuilder;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridToDataValueSetQueryBuilder;
import org.hisp.dhis.integration.t2a.processors.OrganisationUnitQueryBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

/**
 * Pulling program indicators one at a time as DataValueSets (beware if your
 * pool size is too big it breaks the database :-( )
 */
@Component
public class T2ARouter extends RouteBuilder
{
    private static final String CONFIG_D2_USER = "dhis2.api.username";

    private static final String CONFIG_D2_PWD = "dhis2.api.password";

    private static final String CONFIG_PARALLEL = "run.parallel";

    private static final String CONFIG_D2_DV_OU = "org.unit.level";

    private static final String CONFIG_D2_DV_PE = "periods";

    private static final String CONFIG_D2_ANALYTICS = "run.analytics";

    public static final String PROPERTY_PROGRAM_INDICATORS = "pis";

    public static final String PROPERTY_DIMENSIONS = "dimensions";

    public static final String PROPERTY_OU_LEVEL = "ou";

    public static final String PROPERTY_PERIOD = "pe";

    public static final String PROPERTY_ALL_ORG_UNITS = "ous";

    @Override
    public void configure()
        throws Exception
    {
        PropertiesComponent properties = (PropertiesComponent) getContext().getPropertiesComponent();

        // Pool Size
        int poolSize = Runtime.getRuntime().availableProcessors();

        Optional<String> poolSizeProperty = properties.resolveProperty( "thread.pool.size" );

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

        String t2aAuthHeader = getAuthHeader();

        boolean runAnalytics = Boolean
            .parseBoolean( properties.resolveProperty( CONFIG_D2_ANALYTICS ).orElse( "false" ) );

        // adding global auth header and properties
        from( "timer:analytics?repeatCount=1&period=3000000" )
            .setHeader( HttpHeaders.AUTHORIZATION, constant( t2aAuthHeader ) )
            .setProperty( PROPERTY_OU_LEVEL, constant( ou ) )
            .setProperty( PROPERTY_PERIOD, constant( pe ) )
            .to( "direct:t2-auth" );

        from( "direct:t2-auth" )
                .process( new OrganisationUnitQueryBuilder() )
                .toD( "{{dhis2.api.url}}/organisationUnits" )
                .unmarshal().json( OrganisationUnits.class )
                .setProperty( PROPERTY_ALL_ORG_UNITS, simple( "${body}" ) )
                .removeHeader( Exchange.HTTP_QUERY ) // removing query string, so it
                // won't affect the rest of the
                // route
                .to( "direct:t2-ous" );


        // start analytics task
        if ( runAnalytics )
        {
            from( "direct:t2-ous" ).
                setBody().simple("${null}")
                    .setHeader( "CamelHttpMethod", constant( "PUT" ) )
                .log( "Scheduling analytics task" )
                .to( "{{dhis2.api.url}}/resourceTables/analytics" )
                // todo using property instead of headers. Ask Bob!
                .setProperty( "taskId", jsonpath( "$.response.id" ) )
                .to( "direct:t2-analytics" );

            // check for the status in a loop
            from( "direct:t2-analytics" )
                .setHeader( "CamelHttpMethod", constant( "GET" ) )
                .log( "Analytics started with task ID : ${exchangeProperty.taskId}" )
                .toD(
                    "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${exchangeProperty.taskId}" )
                .setProperty( "status", jsonpath( "$[0]" ) )
                .loopDoWhile( simple( "${exchangeProperty.status[completed]} == false" ) )
                // .log( "Inside Loop : ${exchangeProperty.status[completed]}" )
                .toD(
                    "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${exchangeProperty.taskId}" )
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
            .toD( "{{dhis2.api.url}}/programIndicatorGroups/{{pi.group}}" )
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
                .split( method( new DimensionSplitter(), "split" ) )
                .executorService( programIndicatorPool )
                .log( "Processing programIndicator: ${body.period} - ${body.organisationUnit.id} - ${body.programIndicator.id}" )
                .process( new AnalyticsGridQueryBuilder() )
                .setHeader( "Authorization", constant( t2aAuthHeader ) )
                .toD( "{{dhis2.api.url}}/analytics" )
                .process( new AnalyticsGridToDataValueSetQueryBuilder() )
                .setHeader( "Content-Type", constant( MediaType.APPLICATION_JSON_VALUE ) )
                .log( "Posting DataValueSet" )
                .toD( "{{dhis2.api.url}}/dataValueSets" )
                .log("${body}")
                .end()
                .log( "T2A done" );
        }
        else
        {
            throw new IllegalArgumentException( "This implementation supports only parallel execution." );
        }
    }

    private String getAuthHeader()
    {
        PropertiesComponent properties = (PropertiesComponent) getContext().getPropertiesComponent();

        String user = properties.resolveProperty( CONFIG_D2_USER )
            .orElseThrow( () -> new RuntimeException( "Username property is required." ) );
        String pwd = properties.resolveProperty( CONFIG_D2_PWD )
            .orElseThrow( () -> new RuntimeException( "Password property is required." ) );

        return "Basic " + Base64.getEncoder().encodeToString( (user + ":" + pwd).getBytes() );
    }
}
