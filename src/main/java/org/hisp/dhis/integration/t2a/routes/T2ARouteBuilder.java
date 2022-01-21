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
public class T2ARouteBuilder extends RouteBuilder
{
    private static final String CONFIG_D2_USER = "dhis2.api.username";

    private static final String CONFIG_D2_PWD = "dhis2.api.password";

    private static final String CONFIG_D2_DV_OU = "org.unit.level";

    private static final String CONFIG_D2_DV_PE = "periods";

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
        String orgUnitLevel = properties.resolveProperty( CONFIG_D2_DV_OU )
            .orElseThrow( () -> new RuntimeException( CONFIG_D2_DV_OU + " is required" ) );
        String periods = properties.resolveProperty( CONFIG_D2_DV_PE )
            .orElseThrow( () -> new RuntimeException( CONFIG_D2_DV_PE + " is required" ) );
        String authHeader = createAuthHeader();

        buildSourceRoute( authHeader, orgUnitLevel, periods );
        buildFetchOrgUnitsRoute();
        buildScheduleAnalyticsRoute();
        buildPollAnalyticsStatusRoute();
        buildFetchProgramIndicatorsRoute();
        buildPushAggregatedProgramIndicatorsRoute( authHeader );
    }

    protected void buildSourceRoute(String authHeader, String orgUnitLevel, String periods) {
        from( "timer:analytics?repeatCount=1&period=3000000" ).
            setHeader( HttpHeaders.AUTHORIZATION, constant( authHeader ) ).
            setProperty( PROPERTY_OU_LEVEL, constant( orgUnitLevel ) ).
            setProperty( PROPERTY_PERIOD, constant( periods ) ).
            to( "direct:t2-auth" );
    }

    protected void buildPollAnalyticsStatusRoute()
    {
        from( "direct:t2-analytics" ).routeId( "analyticsRoute" ).setHeader( "CamelHttpMethod", constant( "GET" ) )
            .log( "Analytics started with task ID : ${exchangeProperty.taskId}" )
            .toD( "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${exchangeProperty.taskId}" )
            .setProperty( "status", jsonpath( "$[0]" ) )
            .loopDoWhile( simple( "${exchangeProperty.status[completed]} == false" ) )
            .toD( "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${exchangeProperty.taskId}" )
            .setProperty( "status", jsonpath( "$[0]" ) ).process( exchange -> Thread.sleep( 30000 ) ).end()
            .log( "Analytics Task Completed" ).choice()
            .when( simple( "${exchangeProperty.status[level]} == 'ERROR'" ) )
            .throwException( RuntimeException.class, "Analytics Failed : ${body}" ).end()
            .log( "Analytics Status : ${header.status}" );
    }

    protected void buildFetchOrgUnitsRoute() {
        from( "direct:t2-auth" ).process( new OrganisationUnitQueryBuilder() )
            .toD( "{{dhis2.api.url}}/organisationUnits" ).unmarshal().json( OrganisationUnits.class )
            .setProperty( PROPERTY_ALL_ORG_UNITS, simple( "${body}" ) )
            .removeHeader( Exchange.HTTP_QUERY ) // removing query string, so it
            // won't affect the rest of the
            // route
            .setHeader( "skipAggregate", constant( "true" ) )
            .setHeader( "skipEvents", constant( "false" ) )
            .choice().when( simple( "{{run.event.analytics}}" ) ).to( "direct:t2-ous" ).end()
            .to( "direct:analytics-done" );
    }

    protected void buildScheduleAnalyticsRoute()
    {
        from( "direct:t2-ous" ).setBody().simple( "${null}" ).setHeader( "CamelHttpMethod", constant( "PUT" ) )
            .log( "Scheduling analytics task" )
            .toD(
                "{{dhis2.api.url}}/resourceTables/analytics?skipAggregate=${in.header.skipAggregate}&skipEvents=${in.header.skipEvents}" )
            // todo using property instead of headers. Ask Bob!
            .setProperty( "taskId", jsonpath( "$.response.id" ) ).to( "direct:t2-analytics" );
    }

    protected void buildFetchProgramIndicatorsRoute()
    {
        from( "direct:analytics-done" ).setHeader( Exchange.HTTP_METHOD, constant( "GET" ) )
            .setHeader( Exchange.HTTP_QUERY, constant(
                "fields=programIndicators[id,name,aggregateExportCategoryOptionCombo,"
                    + "aggregateExportAttributeOptionCombo,attributeValues]" ) )
            .toD( "{{dhis2.api.url}}/programIndicatorGroups/{{pi.group}}" ).unmarshal()
            .json( ProgramIndicatorGroup.class ).setProperty( PROPERTY_PROGRAM_INDICATORS, simple( "${body}" ) )
            .removeHeader( Exchange.HTTP_QUERY ).to( "direct:pis" );
    }

    protected void buildPushAggregatedProgramIndicatorsRoute( String authHeader )
        throws Exception
    {
        PropertiesComponent propertiesComponent = (PropertiesComponent) getContext().getPropertiesComponent();
        int poolSize = Runtime.getRuntime().availableProcessors();
        Optional<String> poolSizeProperty = propertiesComponent.resolveProperty( "thread.pool.size" );
        if ( poolSizeProperty.isPresent() )
        {
            poolSize = Integer.parseInt( poolSizeProperty.get() );
        }

        ThreadPoolBuilder builder = new ThreadPoolBuilder( getContext() );
        ExecutorService programIndicatorPool = builder.poolSize( poolSize ).maxPoolSize( poolSize ).build();

        from( "direct:pis" ).split( method( new DimensionSplitter(), "split" ) )
            .executorService( programIndicatorPool ).log(
                "Processing program indicator '${body.programIndicator.id}' for period '${body.period}' and organisation unit '${body.organisationUnit.id}'" )
            .process( new AnalyticsGridQueryBuilder() ).setHeader( "Authorization", constant( authHeader ) )
            .toD( "{{dhis2.api.url}}/analytics" ).process( new AnalyticsGridToDataValueSetQueryBuilder() )
            .setHeader( "Content-Type", constant( MediaType.APPLICATION_JSON_VALUE ) ).log( "Posting DataValueSet" )
            .toD( "{{dhis2.api.url}}/dataValueSets" )
            .end()
            .setHeader( "skipAggregate", constant( "false" ) )
            .setHeader( "skipEvents", constant( "true" ) )
            .to( "direct:t2-ous" )
            .log( "Aggregated program indicators" );
    }

    protected String createAuthHeader()
    {
        PropertiesComponent properties = (PropertiesComponent) getContext().getPropertiesComponent();

        String username = properties.resolveProperty( CONFIG_D2_USER )
            .orElseThrow( () -> new RuntimeException( "Username property is required" ) );
        String password = properties.resolveProperty( CONFIG_D2_PWD )
            .orElseThrow( () -> new RuntimeException( "Password property is required" ) );

        return "Basic " + Base64.getEncoder().encodeToString( (username + ":" + password).getBytes() );
    }
}
