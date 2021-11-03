/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.configuration.validation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 *
 */
public class OneOfValidatorTest {
    /**
     *
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testValidate(boolean caseSensitive) {
        // Prepare mocked annotation instance.
        OneOf oneOfAnnotation = mock(OneOf.class);

        when(oneOfAnnotation.value()).thenReturn(new String[]{"foo", "bar"});
        when(oneOfAnnotation.caseSensitive()).thenReturn(caseSensitive);

        // Prepare mocked validation context.
        ValidationContext<String> ctx = mock(ValidationContext.class);

        when(ctx.currentKey()).thenReturn("x");
        when(ctx.getNewValue()).thenReturn("foo", "Bar", "no");

        // Prepare issues captor.
        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);
        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        // Instantiate validator.
        OneOfValidator oneOfValidator = new OneOfValidator();

        // Assert that valid value produces no issues.
        oneOfValidator.validate(oneOfAnnotation, ctx);

        assertThat(issuesCaptor.getAllValues(), is(empty()));

        // Assert that case sencitivity affects validation.
        oneOfValidator.validate(oneOfAnnotation, ctx);

        if (caseSensitive) {
            assertThat(issuesCaptor.getValue().message(), is("'x' configuration value must be one of [foo, bar] (case sensitive)"));
        } else {
            assertThat(issuesCaptor.getAllValues(), is(empty()));
        }

        // Assert that unacceptable value produces validation issue.
        oneOfValidator.validate(oneOfAnnotation, ctx);

        if (caseSensitive) {
            assertThat(issuesCaptor.getValue().message(), is("'x' configuration value must be one of [foo, bar] (case sensitive)"));
        } else {
            assertThat(issuesCaptor.getValue().message(), is("'x' configuration value must be one of [foo, bar] (case insensitive)"));
        }
    }
}
