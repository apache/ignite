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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link ClientExceptionsUtils}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ClientExceptionsUtilsTest {
    /***/
    @Mock
    private ClusterNode node;

    /**
     * Tests that {@link ClientExceptionsUtils#isClientNodeTopologyException(Throwable, ClusterNode)} detects
     * client {@link ClusterTopologyCheckedException}.
     */
    @Test
    public void detectsClientNodeTopologyException() {
        when(node.isClient()).thenReturn(true);

        assertTrue(ClientExceptionsUtils.isClientNodeTopologyException(clusterTopologyCheckedException(), node));
    }

    /**
     * Tests that {@link ClientExceptionsUtils#isClientNodeTopologyException(Throwable, ClusterNode)} returns {@code false}
     * when node is not a client.
     */
    @Test
    public void doesNotDetectsClientNodeTopologyExceptionForNonClient() {
        when(node.isClient()).thenReturn(false);

        assertFalse(ClientExceptionsUtils.isClientNodeTopologyException(clusterTopologyCheckedException(), node));
    }

    /**
     * Tests that {@link ClientExceptionsUtils#isClientNodeTopologyException(Throwable, ClusterNode)} returns {@code false}
     * when exception is not a {@link ClusterTopologyCheckedException}.
     */
    @Test
    public void doesNotDetectClientNodeTopologyExceptionForOtherExceptions() {
        lenient().when(node.isClient()).thenReturn(true);

        assertFalse(ClientExceptionsUtils.isClientNodeTopologyException(new IgniteCheckedException(), node));
    }

    /**
     * Returns a {@link ClusterTopologyCheckedException} that is produced when an attempt to await for an inverse connection
     * fails due to a timeout.
     *
     * @return A {@link ClusterTopologyCheckedException} that is produced when an attempt to await for an inverse connection
     * fails due to a timeout.
     */
    private Exception clusterTopologyCheckedException() {
        return new ClusterTopologyCheckedException(
            "Failed to wait for establishing inverse connection (node left topology): 67cf0e5e-974c-463a-a1f2-915fe3cdd3e7"
        );
    }
}
