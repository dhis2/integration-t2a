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

import java.util.concurrent.ExecutorService;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolBuilder;
import org.hisp.dhis.api.v2_37_6.model.ListGrid;
import org.hisp.dhis.integration.t2a.DimensionSplitter;
import org.hisp.dhis.integration.t2a.processor.AnalyticsGridQueryBuilder;
import org.hisp.dhis.integration.t2a.processor.AnalyticsGridToDataValueSetQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class PushRouteBuilder extends RouteBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PushRouteBuilder.class );

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
        ThreadPoolBuilder builder = new ThreadPoolBuilder( getContext() );
        ExecutorService programIndicatorPool = builder.poolSize( threadPoolSize ).maxPoolSize( threadPoolSize ).build();

        from( "direct:push" ).streamCaching( "true" )
            .split( method( dimensionSplitter, "split" ) )
            .stopOnException()
            .executorService( programIndicatorPool )
            .log( LoggingLevel.INFO, LOGGER,
                "Processing program indicator '${body.programIndicator.id}' for period/s '${body.periods}' and organisation unit/s '${body.organisationUnitIds}'" )
            .process( analyticsGridQueryBuilder )
            .to( "dhis2://get/resource?path=analytics&client=#dhis2Client" ).unmarshal().json( ListGrid.class )
            .process( analyticsGridToDataValueSetQueryBuilder )
            .to( "dhis2://post/resource?path=dataValueSets&inBody=resource&client=#dhis2Client" )
            .log( LoggingLevel.DEBUG, LOGGER, "HTTP POST {{dhis2.api.url}}/dataValueSets Response => ${body}" )
            .end()
            .setHeader( "skipAggregate", constant( "false" ) )
            .setHeader( "skipEvents", constant( "true" ) )
            .to( "direct:run-analytics" );
    }
}
