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
package org.hisp.dhis.integration.t2a.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.hisp.dhis.api.model.v2_37_7.Attribute;
import org.hisp.dhis.api.model.v2_37_7.AttributeValue;
import org.hisp.dhis.api.model.v2_37_7.DataValueSet;
import org.hisp.dhis.api.model.v2_37_7.DataValue__1;
import org.hisp.dhis.api.model.v2_37_7.ListGrid;
import org.hisp.dhis.api.model.v2_37_7.ProgramIndicator;
import org.hisp.dhis.integration.t2a.model.Dimensions;
import org.hisp.dhis.integration.t2a.route.T2ARouteBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith( MockitoExtension.class )
public class AnalyticsGridToDataValueSetQueryBuilderTestCase
{
    @Mock
    private Exchange exchange;

    @Mock
    private Message message;

    @Test
    public void testProcessCopiesAttributeOptionComboToDataValue()
    {
        ListGrid listGrid = new ListGrid().withRows( List.of( List.of( "", "", "", "", "", "", "", "", "" ) ) );

        when( exchange.getMessage() ).thenReturn( message );
        when( exchange.getProperty( T2ARouteBuilder.DIMENSIONS_PROPERTY, Dimensions.class ) ).thenReturn(
            new Dimensions( "2021", "ImspTQPwCqd",
                new ProgramIndicator().withAggregateExportAttributeOptionCombo( "foo" ).withAttributeValues(
                    List.of( new AttributeValue().withValue( "" )
                        .withAttribute( new Attribute().withId( "gWxh7DiRmG7" ) ) ) ) ) );
        when( message.getBody( ListGrid.class ) ).thenReturn( listGrid );

        AnalyticsGridToDataValueSetQueryBuilder analyticsGridToDataValueSetQueryBuilder = new AnalyticsGridToDataValueSetQueryBuilder();
        analyticsGridToDataValueSetQueryBuilder.setAggrDataExportAttrId( "gWxh7DiRmG7" );
        analyticsGridToDataValueSetQueryBuilder.process( exchange );

        ArgumentCaptor<DataValueSet> argumentCaptor = ArgumentCaptor.forClass( DataValueSet.class );
        verify( message ).setBody( argumentCaptor.capture() );
        DataValue__1 dataValue = argumentCaptor.getValue().getDataValues().get().get( 0 );
        assertEquals( "foo", dataValue.getAttributeOptionCombo().get() );
    }

    @Test
    public void testProcessCopiesCategoryOptionComboToDataValue()
    {
        ListGrid listGrid = new ListGrid().withRows( List.of( List.of( "", "", "", "", "", "", "", "", "" ) ) );

        when( exchange.getMessage() ).thenReturn( message );
        when( exchange.getProperty( T2ARouteBuilder.DIMENSIONS_PROPERTY, Dimensions.class ) ).thenReturn(
            new Dimensions( "2021", "ImspTQPwCqd",
                new ProgramIndicator().withAggregateExportCategoryOptionCombo( "bar" ).withAttributeValues(
                    List.of( new AttributeValue().withValue( "" )
                        .withAttribute( new Attribute().withId( "gWxh7DiRmG7" ) ) ) ) ) );
        when( message.getBody( ListGrid.class ) ).thenReturn( listGrid );

        AnalyticsGridToDataValueSetQueryBuilder analyticsGridToDataValueSetQueryBuilder = new AnalyticsGridToDataValueSetQueryBuilder();
        analyticsGridToDataValueSetQueryBuilder.setAggrDataExportAttrId( "gWxh7DiRmG7" );
        analyticsGridToDataValueSetQueryBuilder.process( exchange );

        ArgumentCaptor<DataValueSet> argumentCaptor = ArgumentCaptor.forClass( DataValueSet.class );
        verify( message ).setBody( argumentCaptor.capture() );
        DataValue__1 dataValue = argumentCaptor.getValue().getDataValues().get().get( 0 );
        assertEquals( "bar", dataValue.getCategoryOptionCombo().get() );
    }
}
