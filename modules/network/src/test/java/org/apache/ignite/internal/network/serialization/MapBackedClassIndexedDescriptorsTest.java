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

package org.apache.ignite.internal.network.serialization;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class MapBackedClassIndexedDescriptorsTest {
    private final ClassDescriptorRegistry unrelatedRegistry = new ClassDescriptorRegistry();

    @Test
    void retrievesKnownDescriptorByClass() {
        ClassDescriptor descriptor = unrelatedRegistry.getRequiredDescriptor(String.class);
        var descriptors = new MapBackedClassIndexedDescriptors(Map.of(String.class, descriptor));

        assertThat(descriptors.getDescriptor(String.class), is(descriptor));
    }

    @Test
    void doesNotFindAnythingByClassWhenMapDoesNotContainTheClassDescriptor() {
        var descriptors = new MapBackedClassIndexedDescriptors(emptyMap());

        assertThat(descriptors.getDescriptor(String.class), is(nullValue()));
    }

    @Test
    void throwsWhenQueriedAboutUnknownDescriptorByClass() {
        var descriptors = new MapBackedClassIndexedDescriptors(emptyMap());

        Throwable thrownEx = assertThrows(IllegalStateException.class, () -> descriptors.getRequiredDescriptor(String.class));
        assertThat(thrownEx.getMessage(), startsWith("Did not find a descriptor by class"));
    }
}
