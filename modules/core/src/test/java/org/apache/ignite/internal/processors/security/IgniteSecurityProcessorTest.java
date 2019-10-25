/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link IgniteSecurityProcessor}.
 */
public class IgniteSecurityProcessorTest extends GridCommonAbstractTest {
    /**
     * Checks that {@link IgniteSecurityProcessor#withContext(UUID)} throws exception in case node ID is unknown.
     */
    public void testThrowIllegalStateExceptionIfNodeNotFoundInDiscoCache() {
        GridKernalContext ctx = mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(new IgniteConfiguration());
        when(ctx.discovery()).thenReturn(mock(GridDiscoveryManager.class));

        GridSecurityProcessor secPrc = mock(GridSecurityProcessor.class);

        IgniteSecurityProcessor ignSecPrc = new IgniteSecurityProcessor(ctx, secPrc);

        try {
            ignSecPrc.withContext(UUID.randomUUID());

            fail("IllegalStateException is expected");
        }
        catch (IllegalStateException ignored) {
            // Expected.
        }
    }
}