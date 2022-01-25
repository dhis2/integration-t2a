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

import static org.hisp.dhis.integration.t2a.routes.T2ARouteBuilder.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.camel.Exchange;
import org.apache.camel.spi.PropertiesComponent;
import org.hisp.dhis.integration.t2a.model.Dimensions;
import org.hisp.dhis.integration.t2a.model.OrganisationUnit;
import org.hisp.dhis.integration.t2a.model.OrganisationUnits;
import org.hisp.dhis.integration.t2a.model.ProgramIndicatorGroup;

public class DimensionSplitter
{
    public List<Dimensions> split( Exchange exchange )
    {
        PropertiesComponent propertiesComponent = exchange.getContext().getPropertiesComponent();
        boolean splitOrgUnits = Boolean.parseBoolean( propertiesComponent.resolveProperty(
            SPLIT_ORG_UNITS_CONFIG )
            .orElseThrow( () -> new RuntimeException( SPLIT_ORG_UNITS_CONFIG + " is required" ) ) );
        boolean splitPeriods = Boolean.parseBoolean( propertiesComponent.resolveProperty(
            SPLIT_PERIODS_CONFIG )
            .orElseThrow( () -> new RuntimeException( SPLIT_PERIODS_CONFIG + " is required" ) ) );
        String periods = propertiesComponent.resolveProperty( PERIODS_CONFIG )
            .orElseThrow( () -> new RuntimeException( PERIODS_CONFIG + " is required" ) );

        OrganisationUnits organisationUnits = exchange.getProperty( ALL_ORG_UNITS_PROPERTY,
            OrganisationUnits.class );
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

        List<String> orgUnitIds;
        if ( splitOrgUnits )
        {
            orgUnitIds = organisationUnits.getOrganisationUnits().stream().map( OrganisationUnit::getId )
                .collect( Collectors.toList() );
        }
        else
        {
            orgUnitIds = Arrays.asList( String.join( ";", organisationUnits.getOrganisationUnits().stream()
                .map( OrganisationUnit::getId ).collect( Collectors.toList() ) ) );
        }

        List<Dimensions> dimensions = periodsAsList.stream().flatMap(
            pe -> orgUnitIds.stream()
                .flatMap( ouId -> programIndicatorGroup.getProgramIndicators().stream()
                    .map( pi -> new Dimensions( pe, ouId, pi ) ) ) )
            .collect( Collectors.toList() );

        return dimensions;
    }
}
