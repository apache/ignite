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

package org.apache.ignite.internal.management.ssl;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJobResult;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** */
public class SslReloadTaskTest {
    /** */
    @Test
    public void testReportsTargetThatLeftBeforeMapping() {
        GridTestNode node = new GridTestNode(UUID.randomUUID());
        UUID departedId = UUID.randomUUID();
        SslReloadTask task = new SslReloadTask();

        task.map(List.of(node), new VisorTaskArgument<>(List.of(node.id(), departedId), new NoArg(), false));

        String nodeReport = node.id() + ": no TCP transports started with TLS";
        GridTestJobResult res = new GridTestJobResult(nodeReport, null, null, node, null);

        IgniteException err = assertThrows(IgniteException.class, () -> task.reduce(List.of(res)).result());

        assertEquals(nodeReport + '\n' + departedId + ": left the topology", err.getMessage());
    }

    /** */
    @Test
    public void testReportsAllTargetsThatLeftBeforeMapping() {
        GridTestNode replacement = new GridTestNode(UUID.randomUUID());
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        SslReloadTask task = new SslReloadTask();
        IgniteEx ignite = mock(IgniteEx.class);

        when(ignite.log()).thenReturn(NullLogger.INSTANCE);
        when(ignite.cluster()).thenReturn(mock(IgniteClusterEx.class));
        setFieldValue(task, VisorMultiNodeTask.class, "ignite", ignite);

        IgniteException err = assertThrows(IgniteException.class, () -> task.map(List.of(replacement),
            new VisorTaskArgument<>(List.of(first, second), new NoArg(), false)));

        assertEquals(first + ": left the topology\n" + second + ": left the topology", err.getMessage());
    }

    /** */
    @Test
    public void testReportsJobFailure() {
        GridTestNode node = new GridTestNode(UUID.randomUUID());
        SslReloadTask task = new SslReloadTask();

        task.map(List.of(node), new VisorTaskArgument<>(List.of(node.id()), new NoArg(), false));

        IgniteException jobErr = new IgniteException("Job failed");
        GridTestJobResult res = new GridTestJobResult(null, jobErr, null, node, null);

        IgniteException err = assertThrows(IgniteException.class, () -> task.reduce(List.of(res)).result());

        assertEquals(node.id() + ": failed: " + jobErr.getMessage(), err.getMessage());
    }
}
