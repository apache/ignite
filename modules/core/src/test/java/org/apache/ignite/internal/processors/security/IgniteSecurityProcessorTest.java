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
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link IgniteSecurityProcessor}.
 */
public class IgniteSecurityProcessorTest {
    /**
     * Checks that {@link IgniteSecurityProcessor#withContext(UUID)} throws exception in case a node ID is unknown.
     */
    @Test
    public void testThrowIllegalStateExceptionIfNodeNotFoundInDiscoCache() {
        GridKernalContext ctx = mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(new IgniteConfiguration());
        when(ctx.discovery()).thenReturn(mock(GridDiscoveryManager.class));

        LogListener logLsnr = LogListener
            .matches(s -> s.contains("Failed to obtain a security context."))
            .times(1)
            .build();

        ListeningTestLogger log = new ListeningTestLogger(false);

        log.registerListener(logLsnr);

        when(ctx.log(IgniteSecurityProcessor.class)).thenReturn(log);

        GridSecurityProcessor secPrc = mock(GridSecurityProcessor.class);

        IgniteSecurityProcessor ignSecPrc = new IgniteSecurityProcessor(ctx, secPrc);

        assertThrowsWithCause(() -> ignSecPrc.withContext(UUID.randomUUID()), IllegalStateException.class);

        assertTrue(logLsnr.check());
    }
}
