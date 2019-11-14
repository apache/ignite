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

import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.util.VisorEventMapper;

import static org.apache.ignite.agent.ManagementConsoleProcessor.TOPIC_MANAGEMENT_CONSOLE;
import static org.apache.ignite.events.EventType.EVTS_CACHE_LIFECYCLE;
import static org.apache.ignite.events.EventType.EVTS_CLUSTER_ACTIVATION;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.events.EventType.EVTS_ERROR;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.concat;

/**
 * Events exporter which send events to coordinator.
 */
public class EventsExporter extends GridProcessorAdapter {
    /** Global event types. */
    private static final int[] GLOBAL_EVT_TYPES = concat(EVTS_DISCOVERY, EVTS_CACHE_LIFECYCLE, EVTS_CLUSTER_ACTIVATION);

    /** Local event types. */
    private static final int[] LOCAL_EVT_TYPES = EVTS_ERROR;

    /** Event mapper. */
    private static final VisorEventMapper EVT_MAPPER = new VisorEventMapper();

    /** On node traces listener. */
    private final GridLocalEventListener lsnr = this::processEvent;

    /**
     * @param ctx Context.
     */
    public EventsExporter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Adds local event listener.
     */
    public void addLocalEventListener() {
        this.ctx.event().enableEvents(LOCAL_EVT_TYPES);
        this.ctx.event().addLocalEventListener(lsnr, LOCAL_EVT_TYPES);
    }

    /**
     * Adds global event listener.
     */
    public void addGlobalEventListener() {
        this.ctx.event().enableEvents(GLOBAL_EVT_TYPES);
        this.ctx.event().addLocalEventListener(lsnr, GLOBAL_EVT_TYPES);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        this.ctx.event().removeLocalEventListener(lsnr, concat(LOCAL_EVT_TYPES, GLOBAL_EVT_TYPES));
    }

    /**
     * Local event callback.
     *
     * @param evt local grid event.
     */
    void processEvent(Event evt) {
        VisorGridEvent evt0 = EVT_MAPPER.apply(evt);

        if (evt0 != null)
            ctx.grid()
                .message(ctx.grid().cluster().forOldest())
                .send(TOPIC_MANAGEMENT_CONSOLE, EVT_MAPPER.apply(evt));
    }
}
