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

package org.apache.ignite.agent.processor.export;

import java.util.UUID;
import org.apache.ignite.agent.processor.AbstractServiceTest;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.visor.event.VisorGridDiscoveryEvent;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.apache.ignite.agent.ManagementConsoleProcessor.TOPIC_MANAGEMENT_CONSOLE;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Event exporter test.
 */
public class EventsExporterTest extends AbstractServiceTest {
    /** Context. */
    private GridKernalContext ctx = getMockContext();

    /**
     * Should send events to topic.
     */
    @Test
    public void shouldSendEventsToTopic() {
        EventsExporter exporter = new EventsExporter(ctx);

        GridTestNode rmv = new GridTestNode(UUID.randomUUID());

        DiscoveryEvent evt = new DiscoveryEvent(rmv, "msg", EVT_NODE_LEFT, rmv);

        exporter.processEvent(evt);

        ArgumentCaptor<Object> topicCaptor = ArgumentCaptor.forClass(Object.class);

        ArgumentCaptor<Object> evtsCaptor = ArgumentCaptor.forClass(Object.class);

        verify(ctx.grid().message(), timeout(100).times(1)).send(topicCaptor.capture(), evtsCaptor.capture());

        assertEquals(TOPIC_MANAGEMENT_CONSOLE, topicCaptor.getValue());

        VisorGridDiscoveryEvent actual = (VisorGridDiscoveryEvent) evtsCaptor.getValue();

        assertEquals(evt.type(), actual.getTypeId());
        assertEquals(evt.message(), actual.getMessage());
        assertEquals(rmv.id(), actual.getNid());
    }

    /** {@inheritDoc} */
    @Override protected GridKernalContext getMockContext() {
        GridKernalContext ctx = super.getMockContext();

        when(ctx.event()).thenReturn(mock(GridEventStorageManager.class));

        return ctx;
    }
}
