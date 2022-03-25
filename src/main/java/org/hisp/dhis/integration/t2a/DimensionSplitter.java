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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.camel.Exchange;
import org.hisp.dhis.api.v2_37_6.model.ProgramIndicatorGroup;
import org.hisp.dhis.integration.t2a.model.Dimensions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder.ALL_ORG_UNITS_PROPERTY;

@Component
public class DimensionSplitter
{
    @Value( "${org.unit.batch.size:1}" )
    private int orgUnitBatchSize;

    @Value( "${split.periods:true}" )
    private boolean splitPeriods;

    @Value( "${periods}" )
    private String periods;

    public List<Dimensions> split( Exchange exchange )
    {
        List<String> organisationUnits = exchange.getProperty( ALL_ORG_UNITS_PROPERTY,
            List.class );
        ProgramIndicatorGroup programIndicatorGroup = exchange.getMessage().getBody( ProgramIndicatorGroup.class );
        List<String> periodsAsList;
        if ( splitPeriods )
        {
            periodsAsList = Arrays.stream( periods.split( "," ) ).collect( Collectors.toList() );
        }
        else
        {
            periodsAsList = List.of( String.join( ";", periods.split( "," ) ) );
        }

        List<List<String>> orgUnitBatches = IntStream.iterate( 0,
            i -> i < organisationUnits.size(), i -> i + orgUnitBatchSize )
            .mapToObj( i -> organisationUnits
                .subList( i, Math.min( i + orgUnitBatchSize, organisationUnits.size() ) ) )
            .collect( Collectors.toList() );

        List<Dimensions> dimensions = periodsAsList.stream().flatMap(
            pe -> orgUnitBatches.stream()
                .flatMap( b -> programIndicatorGroup.getProgramIndicators().get().stream()
                    .map( pi -> new Dimensions( pe, String.join( ";", b ), pi ) ) ) )
            .collect( Collectors.toList() );

        return dimensions;
    }

    public int getOrgUnitBatchSize()
    {
        return orgUnitBatchSize;
    }

    public void setOrgUnitBatchSize( int orgUnitBatchSize )
    {
        this.orgUnitBatchSize = orgUnitBatchSize;
    }

    public boolean isSplitPeriods()
    {
        return splitPeriods;
    }

    public void setSplitPeriods( boolean splitPeriods )
    {
        this.splitPeriods = splitPeriods;
    }

    public String getPeriods()
    {
        return periods;
    }

    public void setPeriods( String periods )
    {
        this.periods = periods;
    }
}
