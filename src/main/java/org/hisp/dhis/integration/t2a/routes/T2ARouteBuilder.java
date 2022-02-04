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
import java.util.concurrent.ExecutorService;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolBuilder;
import org.apache.http.HttpHeaders;
import org.hisp.dhis.integration.t2a.DimensionSplitter;
import org.hisp.dhis.integration.t2a.model.OrganisationUnits;
import org.hisp.dhis.integration.t2a.model.ProgramIndicatorGroup;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridQueryBuilder;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridToDataValueSetQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Pulling program indicators one at a time as DataValueSets (beware if your
 * pool size is too big it breaks the database :-( )
 */
@Component
public class T2ARouteBuilder extends RouteBuilder
{
    public static final String PROGRAM_INDICATORS_PROPERTY = "pis";

    public static final String DIMENSIONS_PROPERTY = "dimensions";

    public static final String ALL_ORG_UNITS_PROPERTY = "ous";

    @Value( "${dhis2.api.username}" )
    private String username;

    @Value( "${dhis2.api.password}" )
    private String password;

    @Value( "${thread.pool.size:1}" )
    private int threadPoolSize;

    @Autowired
    private DimensionSplitter dimensionSplitter;

    @Autowired
    private AnalyticsGridToDataValueSetQueryBuilder analyticsGridToDataValueSetQueryBuilder;

    @Override
    public void configure()
        throws Exception
    {
        String authHeader = createAuthHeader();

        buildSourceRoute( authHeader );
        buildFetchOrgUnitsRoute();
        buildScheduleAnalyticsRoute();
        buildPollAnalyticsStatusRoute();
        buildFetchProgramIndicatorsRoute();
        buildPushAggregatedProgramIndicatorsRoute( authHeader );
    }

    protected void buildSourceRoute( String authHeader )
    {
        from( "jetty:{{http.endpoint.uri}}" )
            .removeHeaders( "*" )
            .setHeader( HttpHeaders.AUTHORIZATION, constant( authHeader ) )
            .to( "seda:t2a?waitForTaskToComplete=never" );

        from( "quartz://t2a?cron={{schedule.expression}}" )
            .setHeader( HttpHeaders.AUTHORIZATION, constant( authHeader ) )
            .to( "seda:t2a" );
    }

    protected void buildPollAnalyticsStatusRoute()
    {
        from( "direct:poll-analytics" ).routeId( "pollAnalyticsRoute" ).streamCaching( "true" )
            .setHeader( "CamelHttpMethod", constant( "GET" ) )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Analytics started with task ID ${header.taskId}" )
            .toD( "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${header.taskId}" )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP GET {{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${header.taskId} => ${body}" )
            .setProperty( "status", jsonpath( "$[0]" ) )
            .loopDoWhile( simple( "${exchangeProperty.status[completed]} == false" ) )
            .toD( "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${header.taskId}" )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP GET {{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${header.taskId} => ${body}" )
            .setProperty( "status", jsonpath( "$[0]" ) ).delay( 30000 ).end()
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Analytics task completed" )
            .choice()
            .when( simple( "${exchangeProperty.status[level]} == 'ERROR'" ) )
            .throwException( RuntimeException.class, "Analytics failed => ${body}" ).end()
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Analytics status: ${header.status}" );
    }

    protected void buildFetchOrgUnitsRoute()
    {
        from( "seda:t2a" ).setProperty( "startTime", simple( "${bean:java.lang.System?method=currentTimeMillis}" ) )
            .streamCaching( "true" )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Fetching organisation units..." )
            .setHeader( Exchange.HTTP_QUERY, simple( "paging=false&fields=id&filter=level:eq:{{org.unit.level}}" ) )
            .toD( "{{dhis2.api.url}}/organisationUnits" )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP GET {{dhis2.api.url}}/organisationUnits => ${body}" )
            .unmarshal().json( OrganisationUnits.class )
            .setProperty( ALL_ORG_UNITS_PROPERTY, simple( "${body}" ) )
            .removeHeader( Exchange.HTTP_QUERY ) // removing query string, so it
            // won't affect the rest of the
            // route
            .setHeader( "skipAggregate", constant( "true" ) )
            .setHeader( "skipEvents", constant( "false" ) )
            .choice().when( simple( "{{run.event.analytics}}" ) ).to( "direct:run-analytics" ).end()
            .to( "direct:collect" )
            .process( e -> e.getIn()
                .setBody( (System.currentTimeMillis() - e.getProperty( "startTime", Long.class )) / 1000 ) )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Aggregated program indicators in ${body} seconds" );
    }

    protected void buildScheduleAnalyticsRoute()
    {
        from( "direct:run-analytics" ).setBody().simple( "${null}" ).setHeader( "CamelHttpMethod", constant( "PUT" ) )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Scheduling analytics task..." )
            .toD(
                "{{dhis2.api.url}}/resourceTables/analytics?skipAggregate=${header.skipAggregate}&skipEvents=${header.skipEvents}&lastYears={{analytics.last.years}}" )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP PUT {{dhis2.api.url}}/resourceTables/analytics?skipAggregate=${header.skipAggregate}&skipEvents=${header.skipEvents}&lastYears={{analytics.last.years}} => ${body}" )
            .setHeader( "taskId", jsonpath( "$.response.id" ) ).to( "direct:poll-analytics" );
    }

    protected void buildFetchProgramIndicatorsRoute()
    {
        from( "direct:collect" ).streamCaching( "true" ).setHeader( Exchange.HTTP_METHOD, constant( "GET" ) )
            .setHeader( Exchange.HTTP_QUERY, constant(
                "fields=programIndicators[id,name,aggregateExportCategoryOptionCombo,"
                    + "aggregateExportAttributeOptionCombo,attributeValues]" ) )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Fetching program indicators..." )
            .toD( "{{dhis2.api.url}}/programIndicatorGroups/{{pi.group.id}}" )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP GET {{dhis2.api.url}}/programIndicatorGroups/{{pi.group.id}} => ${body}" )
            .unmarshal()
            .json( ProgramIndicatorGroup.class ).setProperty( PROGRAM_INDICATORS_PROPERTY, simple( "${body}" ) )
            .removeHeader( Exchange.HTTP_QUERY ).to( "direct:push" );
    }

    protected void buildPushAggregatedProgramIndicatorsRoute( String authHeader )
        throws Exception
    {
        ThreadPoolBuilder builder = new ThreadPoolBuilder( getContext() );
        ExecutorService programIndicatorPool = builder.poolSize( threadPoolSize ).maxPoolSize( threadPoolSize ).build();

        from( "direct:push" ).streamCaching( "true" ).split( method( dimensionSplitter, "split" ) )
            .executorService( programIndicatorPool )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Processing program indicator '${body.programIndicator.id}' for period/s '${body.periods}' and organisation unit/s '${body.organisationUnitIds}'" )
            .process( new AnalyticsGridQueryBuilder() ).setHeader( "Authorization", constant( authHeader ) )
            .toD( "{{dhis2.api.url}}/analytics" ).process( analyticsGridToDataValueSetQueryBuilder )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP GET {{dhis2.api.url}}/analytics => ${body}" )
            .setHeader( "Content-Type", constant( "application/json" ) )
            .toD( "{{dhis2.api.url}}/dataValueSets" )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP POST {{dhis2.api.url}}/dataValueSets => ${body}" )
            .end()
            .setHeader( "skipAggregate", constant( "false" ) )
            .setHeader( "skipEvents", constant( "true" ) )
            .to( "direct:run-analytics" );
    }

    protected String createAuthHeader()
    {
        return "Basic " + Base64.getEncoder().encodeToString( (username + ":" + password).getBytes() );
    }
}
