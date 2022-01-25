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
import org.apache.http.HttpHeaders;
import org.hisp.dhis.integration.t2a.DimensionSplitter;
import org.hisp.dhis.integration.t2a.model.OrganisationUnits;
import org.hisp.dhis.integration.t2a.model.ProgramIndicatorGroup;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridQueryBuilder;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridToDataValueSetQueryBuilder;
import org.hisp.dhis.integration.t2a.processors.OrganisationUnitQueryBuilder;
import org.springframework.stereotype.Component;

/**
 * Pulling program indicators one at a time as DataValueSets (beware if your
 * pool size is too big it breaks the database :-( )
 */
@Component
public class T2ARouteBuilder extends RouteBuilder
{
    public static final String SPLIT_ORG_UNITS_CONFIG = "split.org.units";

    public static final String SPLIT_PERIODS_CONFIG = "split.periods";

    public static final String ORG_UNIT_LEVEL_CONFIG = "org.unit.level";

    public static final String PERIODS_CONFIG = "periods";

    public static final String PROGRAM_INDICATORS_PROPERTY = "pis";

    public static final String DIMENSIONS_PROPERTY = "dimensions";

    public static final String ALL_ORG_UNITS_PROPERTY = "ous";

    private static final String DHIS2_API_USERNAME_CONFIG = "dhis2.api.username";

    private static final String DHIS2_API_PWD_CONFIG = "dhis2.api.password";

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
        from( "jetty:{{http.endpoint.uri}}?exchangePattern=InOnly" )
            .removeHeaders( "*" )
            .setHeader( HttpHeaders.AUTHORIZATION, constant( authHeader ) )
            .to( "direct:t2-auth" );

        from( "quartz://t2a?cron={{schedule.expression}}" )
            .setHeader( HttpHeaders.AUTHORIZATION, constant( authHeader ) )
            .to( "direct:t2-auth" );
    }

    protected void buildPollAnalyticsStatusRoute()
    {
        from( "direct:t2-analytics" ).routeId( "analyticsRoute" ).setHeader( "CamelHttpMethod", constant( "GET" ) )
            .log( "Analytics started with task ID : ${header.taskId}" )
            .toD( "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${header.taskId}" )
            .setProperty( "status", jsonpath( "$[0]" ) )
            .loopDoWhile( simple( "${exchangeProperty.status[completed]} == false" ) )
            .toD( "{{dhis2.api.url}}/system/tasks/ANALYTICS_TABLE/${header.taskId}" )
            .setProperty( "status", jsonpath( "$[0]" ) ).process( exchange -> Thread.sleep( 30000 ) ).end()
            .log( "Analytics Task Completed" ).choice()
            .when( simple( "${exchangeProperty.status[level]} == 'ERROR'" ) )
            .throwException( RuntimeException.class, "Analytics Failed : ${body}" ).end()
            .log( "Analytics Status : ${header.status}" );
    }

    protected void buildFetchOrgUnitsRoute()
    {
        from( "direct:t2-auth" ).process( new OrganisationUnitQueryBuilder() )
            .toD( "{{dhis2.api.url}}/organisationUnits" ).unmarshal().json( OrganisationUnits.class )
            .setProperty( ALL_ORG_UNITS_PROPERTY, simple( "${body}" ) )
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
                "{{dhis2.api.url}}/resourceTables/analytics?skipAggregate=${header.skipAggregate}&skipEvents=${header.skipEvents}" )
            .setHeader( "taskId", jsonpath( "$.response.id" ) ).to( "direct:t2-analytics" );
    }

    protected void buildFetchProgramIndicatorsRoute()
    {
        from( "direct:analytics-done" ).setHeader( Exchange.HTTP_METHOD, constant( "GET" ) )
            .setHeader( Exchange.HTTP_QUERY, constant(
                "fields=programIndicators[id,name,aggregateExportCategoryOptionCombo,"
                    + "aggregateExportAttributeOptionCombo,attributeValues]" ) )
            .toD( "{{dhis2.api.url}}/programIndicatorGroups/{{pi.group.id}}" ).unmarshal()
            .json( ProgramIndicatorGroup.class ).setProperty( PROGRAM_INDICATORS_PROPERTY, simple( "${body}" ) )
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
                "Processing program indicator '${body.programIndicator.id}' for period/s '${body.periods}' and organisation unit/s '${body.organisationUnitIds}'" )
            .process( new AnalyticsGridQueryBuilder() ).setHeader( "Authorization", constant( authHeader ) )
            .toD( "{{dhis2.api.url}}/analytics" ).process( new AnalyticsGridToDataValueSetQueryBuilder() )
            .setHeader( "Content-Type", constant( "application/json" ) ).log( "Posting DataValueSet..." )
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

        String username = properties.resolveProperty( DHIS2_API_USERNAME_CONFIG )
            .orElseThrow( () -> new RuntimeException( "Username property is required" ) );
        String password = properties.resolveProperty( DHIS2_API_PWD_CONFIG )
            .orElseThrow( () -> new RuntimeException( "Password property is required" ) );

        return "Basic " + Base64.getEncoder().encodeToString( (username + ":" + password).getBytes() );
    }
}
