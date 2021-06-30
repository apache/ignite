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

package org.apache.ignite.internal.processors.security.systemview;

import java.util.Collections;
import java.util.Iterator;
import java.util.stream.StreamSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.spi.systemview.view.ClusterNodeView;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.PartitionStateView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODES_SYS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.PART_STATES_VIEW;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests authorized access to {@link SystemView}s.
 */
public class SystemViewAuthorizationTest extends AbstractSecurityTest {
    /** @throws Exception If failed. */
    @Test
    public void testNodesCanStartWithoutPermission() throws Exception {
        SecurityPermissionSet permSet = new SecurityPermissionSetBuilder().build();

        Ignite srv = startGrid("server", permSet, false);
        startGrid("client", permSet, true);

        assertEquals(2, srv.cluster().nodes().size());
    }

    /** @throws Exception If failed. */
    @Test
    public void testCanReadViewWhenPermitted() throws Exception {
        SecurityPermissionSet permSet = new SecurityPermissionSetBuilder()
            .appendSystemPermissions(ADMIN_VIEW)
            .build();

        IgniteEx server = startGrid("server", permSet, false);
        IgniteEx client = startGrid("client", permSet, true);

        checkReadNodesViewSuceeds(server, 2);
        checkReadNodesViewSuceeds(client, 2);

        checkReadPartitionsViewSucceeds(server);
        checkReadPartitionsViewSucceeds(client);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCannotReadViewWithoutPermission() throws Exception {
        SecurityPermissionSet permSet = new SecurityPermissionSetBuilder().build();

        IgniteEx server = startGrid("server", permSet, false);
        IgniteEx client = startGrid("client", permSet, true);

        checkReadNodesViewFails(server);
        checkReadNodesViewFails(client);

        checkReadPartitionsViewFails(server);
        checkReadPartitionsViewFails(client);
    }

    /** */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    private static void checkReadNodesViewSuceeds(IgniteEx ignite, int expectedNodeCnt) {
        SystemView<ClusterNodeView> view = ignite.context().systemView().view(NODES_SYS_VIEW);

        assertEquals(expectedNodeCnt, view.size());

        log.info(String.format("View [name=%s, desc=%s, size=%d]", view.name(), view.description(), view.size()));

        assertEquals(expectedNodeCnt, StreamSupport.stream(view.spliterator(), false).count());
    }

    /** */
    private static void checkReadPartitionsViewSucceeds(IgniteEx ignite) {
        SystemView<PartitionStateView> v = ignite.context().systemView().view(PART_STATES_VIEW);

        FiltrableSystemView<PartitionStateView> view = (FiltrableSystemView<PartitionStateView>)v;

        log.info(String.format("View [name=%s, desc=%s, size=%d]", view.name(), view.description(), view.size()));

        for (Iterator<PartitionStateView> iter = view.iterator(Collections.emptyMap()); iter.hasNext(); )
            log.info(GridToStringBuilder.toString(PartitionStateView.class, iter.next()));
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    private static void checkReadNodesViewFails(IgniteEx ignite) {
        SystemView<ClusterNodeView> view = ignite.context().systemView().view(NODES_SYS_VIEW);

        assertThrowsWithCause(view::size, SecurityException.class);
        assertThrowsWithCause(view::iterator, SecurityException.class);
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    private static void checkReadPartitionsViewFails(IgniteEx ignite) {
        SystemView<PartitionStateView> v = ignite.context().systemView().view(PART_STATES_VIEW);

        FiltrableSystemView<PartitionStateView> view = (FiltrableSystemView<PartitionStateView>)v;

        assertThrowsWithCause(view::size, SecurityException.class);
        assertThrowsWithCause(() -> view.iterator(Collections.emptyMap()), SecurityException.class);
    }
}
