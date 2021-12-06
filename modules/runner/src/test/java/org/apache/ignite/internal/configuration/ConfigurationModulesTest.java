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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.RootKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigurationModulesTest {
    @Mock
    private RootKey<?, ?> rootKeyA;
    @Mock
    private RootKey<?, ?> rootKeyB;
    @Mock
    private RootKey<?, ?> rootKeyC;
    @Mock
    private RootKey<?, ?> rootKeyD;

    @Mock
    private ConfigurationModule moduleA;
    @Mock
    private ConfigurationModule moduleB;
    @Mock
    private ConfigurationModule moduleC;
    @Mock
    private ConfigurationModule moduleD;

    @Test
    void mergesLocalModules() {
        when(moduleA.type()).thenReturn(LOCAL);
        when(moduleB.type()).thenReturn(LOCAL);
        when(moduleC.type()).thenReturn(DISTRIBUTED);
        when(moduleD.type()).thenReturn(DISTRIBUTED);
        when(moduleA.rootKeys()).thenReturn(Set.of(rootKeyA));
        when(moduleB.rootKeys()).thenReturn(Set.of(rootKeyB));

        var modules = new ConfigurationModules(List.of(moduleA, moduleB, moduleC, moduleD));

        assertThat(modules.local().rootKeys(), containsInAnyOrder(rootKeyA, rootKeyB));
    }

    @Test
    void mergesDistributedModules() {
        when(moduleA.type()).thenReturn(LOCAL);
        when(moduleB.type()).thenReturn(LOCAL);
        when(moduleC.type()).thenReturn(DISTRIBUTED);
        when(moduleD.type()).thenReturn(DISTRIBUTED);
        when(moduleC.rootKeys()).thenReturn(Set.of(rootKeyC));
        when(moduleD.rootKeys()).thenReturn(Set.of(rootKeyD));

        var modules = new ConfigurationModules(List.of(moduleA, moduleB, moduleC, moduleD));

        assertThat(modules.distributed().rootKeys(), containsInAnyOrder(rootKeyC, rootKeyD));
    }
}
