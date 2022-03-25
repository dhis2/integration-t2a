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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolBuilder;
import org.hisp.dhis.api.v2_37_6.model.ListGrid;
import org.hisp.dhis.api.v2_37_6.model.ProgramIndicatorGroup;
import org.hisp.dhis.integration.t2a.DimensionSplitter;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridQueryBuilder;
import org.hisp.dhis.integration.t2a.processors.AnalyticsGridToDataValueSetQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger( T2ARouteBuilder.class );

    public static final String DIMENSIONS_PROPERTY = "dimensions";

    public static final String ALL_ORG_UNITS_PROPERTY = "ous";

    @Value( "${thread.pool.size:1}" )
    private int threadPoolSize;

    @Autowired
    private DimensionSplitter dimensionSplitter;

    @Autowired
    private AnalyticsGridToDataValueSetQueryBuilder analyticsGridToDataValueSetQueryBuilder;

    @Autowired
    private AnalyticsGridQueryBuilder analyticsGridQueryBuilder;

    @Override
    public void configure()
        throws Exception
    {
        buildSourceRoute();
        buildFetchOrgUnitsRoute();
        buildScheduleAnalyticsRoute();
        buildFetchProgramIndicatorsRoute();
        buildPushAggregatedProgramIndicatorsRoute();
    }

    protected void buildSourceRoute()
    {
        from( "jetty:{{http.endpoint.uri:http://localhost:8081/dhis2/t2a}}" )
            .removeHeaders( "*" )
            .to( "seda:t2a?waitForTaskToComplete=never" );

        from( "quartz://t2a?cron={{schedule.expression:0 0 0 * * ?}}" )
            .to( "seda:t2a" );
    }

    protected void buildFetchOrgUnitsRoute()
    {
        from( "seda:t2a" ).setProperty( "startTime", simple( "${bean:java.lang.System?method=currentTimeMillis}" ) )
            .streamCaching( "true" )
            .log( LoggingLevel.INFO, LOGGER, "Fetching organisation units..." )
            .toD(
                "dhis2://get/resource?path=organisationUnits&fields=id&filter=level:eq:{{org.unit.level}}&client=#dhis2Client" )
            .setProperty( ALL_ORG_UNITS_PROPERTY, jsonpath( "$.organisationUnits..id" ) )
            .setHeader( "skipAggregate", constant( "true" ) )
            .setHeader( "skipEvents", constant( "false" ) )
            .choice().when( simple( "{{run.event.analytics:true}}" ) ).to( "direct:run-analytics" ).end()
            .to( "direct:collect" )
            .process( e -> e.getIn()
                .setBody( (System.currentTimeMillis() - e.getProperty( "startTime", Long.class )) / 1000 ) )
            .log( LoggingLevel.INFO, LOGGER, "Aggregated program indicators in ${body} seconds" );
    }

    protected void buildScheduleAnalyticsRoute()
    {
        from( "direct:run-analytics" ).routeId( "scheduleAnalyticsRoute" )
            .log( LoggingLevel.INFO, LOGGER, "Scheduling analytics task..." )
            .setHeader( "CamelDhis2.queryParams", constant( Map.of( "cacheClear", List.of( "true" ) ) ) )
            .toD( "dhis2://post/resource?path=maintenance&client=#dhis2Client" )
            .toD(
                "dhis2://resourceTables/analytics?skipAggregate=${header.skipAggregate}&skipEvents=${header.skipEvents}&lastYears={{analytics.last.years:1}}&client=#dhis2Client" )
            .log( LoggingLevel.INFO, LOGGER, "Analytics task completed" );
    }

    protected void buildFetchProgramIndicatorsRoute()
    {
        from( "direct:collect" ).log( LoggingLevel.INFO, LOGGER, "Fetching program indicators..." )
            .toD(
                "dhis2://get/resource?path=programIndicatorGroups/{{pi.group.id}}&fields=programIndicators[id,name,aggregateExportCategoryOptionCombo,aggregateExportAttributeOptionCombo,attributeValues]&client=#dhis2Client" )
            .unmarshal().json( ProgramIndicatorGroup.class )
            .to( "direct:push" );
    }

    protected void buildPushAggregatedProgramIndicatorsRoute()
        throws Exception
    {
        ThreadPoolBuilder builder = new ThreadPoolBuilder( getContext() );
        ExecutorService programIndicatorPool = builder.poolSize( threadPoolSize ).maxPoolSize( threadPoolSize ).build();

        from( "direct:push" ).streamCaching( "true" )
            .split( method( dimensionSplitter, "split" ) )
                .executorService( programIndicatorPool )
                .log( LoggingLevel.INFO,
                    LOGGER,
                    "Processing program indicator '${body.programIndicator.id}' for period/s '${body.periods}' and organisation unit/s '${body.organisationUnitIds}'" )
                .process( analyticsGridQueryBuilder )
                .toD( "dhis2://get/resource?path=analytics&client=#dhis2Client" ).unmarshal().json( ListGrid.class )
                .process( analyticsGridToDataValueSetQueryBuilder )
                .toD( "dhis2://post/resource?path=dataValueSets&inBody=resource&client=#dhis2Client" )
                .log( LoggingLevel.DEBUG,
                    LOGGER,
                    "HTTP POST {{dhis2.api.url}}/dataValueSets Response => ${body}" )
            .end()
            .setHeader( "skipAggregate", constant( "false" ) )
            .setHeader( "skipEvents", constant( "true" ) )
            .to( "direct:run-analytics" );
    }
}
