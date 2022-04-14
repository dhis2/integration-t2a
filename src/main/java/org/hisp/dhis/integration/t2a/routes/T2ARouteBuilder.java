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

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.http.HttpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Pulling program indicators one at a time as DataValueSets (beware if your
 * pool size is too big it breaks the database :-( )
 */
@Component
public class T2ARouteBuilder extends RouteBuilder
{
    @Value( "${dhis2.api.username}" )
    private String username;

    @Value( "${dhis2.api.password}" )
    private String password;

    @Override
    public void configure()
        throws Exception
    {
        String authHeader = createAuthHeader();

        buildSourceRoute( authHeader );
        buildMainRoute();
        buildScheduleAnalyticsRoute();
        buildPollAnalyticsStatusRoute();
    }

    protected void buildSourceRoute( String authHeader )
    {
        from( "jetty:{{http.endpoint.uri:http://localhost:8081/dhis2/t2a}}" )
            .removeHeaders( "*" )
            .setHeader( HttpHeaders.AUTHORIZATION, constant( authHeader ) )
            .to( "seda:t2a?waitForTaskToComplete=never" );
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
            .setProperty( "status", jsonpath( "$[0]" ) ).delay( 10000 ).end()
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Analytics task completed" )
            .choice()
            .when( simple( "${exchangeProperty.status[level]} == 'ERROR'" ) )
            .throwException( RuntimeException.class, "Analytics failed => ${body}" ).end()
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Analytics status: ${header.status}" )
            .end()
            .setHeader( "Content-Type", constant( "application/json" ) )
            .setHeader( "CamelHttpMethod", constant( "POST" ) )
            .setBody( simple( "${null}" ) )
            .toD( "{{dhis2.api.url}}/maintenance?cacheClear=true" );
    }

    protected void buildMainRoute()
    {
        from( "seda:t2a" ).routeId( "t2aRoute" )
            .setProperty( "startTime", simple( "${bean:java.lang.System?method=currentTimeMillis}" ) )
            .streamCaching( "true" )
            .setHeader( "skipAggregate", constant( "true" ) )
            .setHeader( "skipEvents", constant( "false" ) )
            .choice().when( simple( "{{run.event.analytics:true}}" ) ).to( "direct:run-analytics" ).end()
            .process( e -> e.getIn()
                .setBody( (System.currentTimeMillis() - e.getProperty( "startTime", Long.class )) / 1000 ) )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Aggregated program indicators in ${body} seconds" );
    }

    protected void buildScheduleAnalyticsRoute()
    {
        from( "direct:run-analytics" )
            .setHeader( "Content-Type", constant( "application/json" ) )
            .setHeader( "CamelHttpMethod", constant( "POST" ) )
            .setBody( simple( "${null}" ) )
            .toD( "{{dhis2.api.url}}/maintenance?cacheClear=true" )
            .log( LoggingLevel.INFO,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "Scheduling analytics task..." )
            .toD(
                "{{dhis2.api.url}}/resourceTables/analytics?skipAggregate=${header.skipAggregate}&skipEvents=${header.skipEvents}&lastYears={{analytics.last.years:1}}" )
            .log( LoggingLevel.DEBUG,
                org.slf4j.LoggerFactory.getLogger( "org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder" ),
                "HTTP POST {{dhis2.api.url}}/resourceTables/analytics?skipAggregate=${header.skipAggregate}&skipEvents=${header.skipEvents}&lastYears={{analytics.last.years:1}} => ${body}" )
            .setHeader( "taskId", jsonpath( "$.response.id" ) ).to( "direct:poll-analytics" );
    }

    protected String createAuthHeader()
    {
        return "Basic " + Base64.getEncoder().encodeToString( (username + ":" + password).getBytes() );
    }
}
