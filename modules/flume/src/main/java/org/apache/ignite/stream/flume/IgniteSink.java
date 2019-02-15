/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.stream.flume;

import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume sink for Apache Ignite.
 */
public class IgniteSink extends AbstractSink implements Configurable {
    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteSink.class);

    /** Default batch size. */
    private static final int DFLT_BATCH_SIZE = 100;

    /** Ignite configuration file. */
    private String springCfgPath;

    /** Cache name. */
    private String cacheName;

    /** Event transformer implementation class. */
    private String eventTransformerCls;

    /** Number of events to be written per Flume transaction. */
    private int batchSize;

    /** Monitoring counter. */
    private SinkCounter sinkCounter;

    /** Event transformer. */
    private EventTransformer<Event, Object, Object> eventTransformer;

    /** Ignite instance. */
    private Ignite ignite;

    /** Empty constructor. */
    public IgniteSink() {
    }

    /**
     * Sink configurations with Ignite-specific settings.
     *
     * @param context Context for sink.
     */
    @Override public void configure(Context context) {
        springCfgPath = context.getString(IgniteSinkConstants.CFG_PATH);
        cacheName = context.getString(IgniteSinkConstants.CFG_CACHE_NAME);
        eventTransformerCls = context.getString(IgniteSinkConstants.CFG_EVENT_TRANSFORMER);
        batchSize = context.getInteger(IgniteSinkConstants.CFG_BATCH_SIZE, DFLT_BATCH_SIZE);

        if (sinkCounter == null)
            sinkCounter = new SinkCounter(getName());
    }

    /**
     * Starts a grid and initializes an event transformer.
     */
    @SuppressWarnings("unchecked")
    @Override public synchronized void start() {
        A.notNull(springCfgPath, "Ignite config file");
        A.notNull(cacheName, "Cache name");
        A.notNull(eventTransformerCls, "Event transformer class");

        sinkCounter.start();

        try {
            if (ignite == null)
                ignite = Ignition.start(springCfgPath);

            if (eventTransformerCls != null && !eventTransformerCls.isEmpty()) {
                Class<? extends EventTransformer> clazz =
                    (Class<? extends EventTransformer<Event, Object, Object>>)Class.forName(eventTransformerCls);

                eventTransformer = clazz.newInstance();
            }
        }
        catch (Exception e) {
            log.error("Failed to start grid", e);

            sinkCounter.incrementConnectionFailedCount();

            throw new FlumeException("Failed to start grid", e);
        }

        sinkCounter.incrementConnectionCreatedCount();

        super.start();
    }

    /**
     * Stops the grid.
     */
    @Override public synchronized void stop() {
        if (ignite != null)
            ignite.close();

        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();

        super.stop();
    }

    /**
     * Processes Flume events.
     */
    @Override public Status process() throws EventDeliveryException {
        Channel channel = getChannel();

        Transaction transaction = channel.getTransaction();

        int eventCount = 0;

        try {
            transaction.begin();

            List<Event> batch = new ArrayList<>(batchSize);

            for (; eventCount < batchSize; ++eventCount) {
                Event event = channel.take();

                if (event == null) {
                    break;
                }

                batch.add(event);
            }

            if (!batch.isEmpty()) {
                ignite.cache(cacheName).putAll(eventTransformer.transform(batch));

                if (batch.size() < batchSize)
                    sinkCounter.incrementBatchUnderflowCount();
                else
                    sinkCounter.incrementBatchCompleteCount();
            }
            else {
                sinkCounter.incrementBatchEmptyCount();
            }

            sinkCounter.addToEventDrainAttemptCount(batch.size());

            transaction.commit();

            sinkCounter.addToEventDrainSuccessCount(batch.size());
        }
        catch (Exception e) {
            log.error("Failed to process events", e);

            try {
                transaction.rollback();
            }
            catch (Throwable e1) {
                e.addSuppressed(e1);
            }

            throw new EventDeliveryException(e);
        }
        finally {
            transaction.close();
        }

        return eventCount == 0 ? Status.BACKOFF : Status.READY;
    }
}
