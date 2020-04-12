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

package org.apache.ignite.internal.processors.security;

import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link IgniteSecurityProcessor}.
 */
public class IgniteSecurityProcessorTest {
    /**
     * Checks that {@link IgniteSecurityProcessor#withContext(UUID)} throws exception in case a node ID is unknown.
     */
    @Test(expected = IllegalStateException.class)
    public void testThrowIllegalStateExceptionIfNodeNotFoundInDiscoCache() {
        GridKernalContext ctx = mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(new IgniteConfiguration());
        when(ctx.discovery()).thenReturn(mock(GridDiscoveryManager.class));

        GridSecurityProcessor secPrc = mock(GridSecurityProcessor.class);

        IgniteSecurityProcessor ignSecPrc = new IgniteSecurityProcessor(ctx, secPrc);

        ignSecPrc.withContext(UUID.randomUUID());
    }
}
