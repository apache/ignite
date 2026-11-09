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
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask.SslReloadJobResult;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/** */
public class SslReloadTaskTest {
    /** */
    @Test
    public void testReportsTargetThatLeftBeforeMapping() {
        GridTestNode node = new GridTestNode(UUID.randomUUID());
        UUID departedId = UUID.randomUUID();
        SslReloadTask task = new SslReloadTask();

        task.map(List.of(node), new VisorTaskArgument<>(List.of(node.id(), departedId), new NoArg(), false));

        String nodeReport = node.id() + ": TLS is not enabled";
        GridTestJobResult res = new GridTestJobResult(new SslReloadJobResult(nodeReport, false), null, null, node, null);

        IgniteException err = assertThrows(IgniteException.class, () -> task.reduce(List.of(res)).result());

        assertEquals(nodeReport + '\n' + departedId + ": left the topology", err.getMessage());
    }

    /** */
    @Test
    public void testJobFailureDoesNotExposeArgumentErrorCause() {
        GridTestNode node = new GridTestNode(UUID.randomUUID());
        SslReloadTask task = new SslReloadTask();

        task.map(List.of(node), new VisorTaskArgument<>(List.of(node.id()), new NoArg(), false));

        IgniteException jobErr = new IgniteException(new IllegalArgumentException("Invalid source setting"));
        GridTestJobResult res = new GridTestJobResult(null, jobErr, null, node, null);

        IgniteException err = assertThrows(IgniteException.class, () -> task.reduce(List.of(res)).result());

        assertEquals(node.id() + ": failed: " + jobErr.getMessage(), err.getMessage());
        assertNull(err.getCause());
    }
}
