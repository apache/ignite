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

package org.apache.ignite.internal.processors.platform.events;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventAdapter;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformEventFilterListener;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Interop events.
 */
public class PlatformEvents extends PlatformAbstractTarget {
    /** */
    private static final int OP_REMOTE_QUERY = 1;

    /** */
    private static final int OP_REMOTE_LISTEN = 2;

    /** */
    private static final int OP_STOP_REMOTE_LISTEN = 3;

    /** */
    private static final int OP_WAIT_FOR_LOCAL = 4;

    /** */
    private static final int OP_LOCAL_QUERY = 5;

    /** */
    private static final int OP_RECORD_LOCAL = 6;

    /** */
    private static final int OP_ENABLE_LOCAL = 8;

    /** */
    private static final int OP_DISABLE_LOCAL = 9;

    /** */
    private static final int OP_GET_ENABLED_EVENTS = 10;

    /** */
    private static final int OP_WITH_ASYNC = 11;

    /** */
    private static final int OP_IS_ENABLED = 12;

    /** */
    private static final int OP_LOCAL_LISTEN = 13;

    /** */
    private static final int OP_STOP_LOCAL_LISTEN = 14;

    /** */
    private static final int OP_REMOTE_QUERY_ASYNC = 15;

    /** */
    private static final int OP_WAIT_FOR_LOCAL_ASYNC = 16;

    /** */
    private final IgniteEvents events;

    /** */
    private final IgniteEvents eventsAsync;

    /** */
    private final EventResultWriter eventResWriter;

    /** */
    private final EventCollectionResultWriter eventColResWriter;

    /**
     * Ctor.
     *
     * @param platformCtx Context.
     * @param events Ignite events.
     */
    public PlatformEvents(PlatformContext platformCtx, IgniteEvents events) {
        super(platformCtx);

        assert events != null;

        this.events = events;
        eventsAsync = events.withAsync();

        eventResWriter = new EventResultWriter(platformCtx);
        eventColResWriter = new EventCollectionResultWriter(platformCtx);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_RECORD_LOCAL:
                // TODO: IGNITE-1410.
                return TRUE;

            case OP_ENABLE_LOCAL:

                events.enableLocal(readEventTypes(reader));

                return TRUE;

            case OP_DISABLE_LOCAL:

                events.disableLocal(readEventTypes(reader));

                return TRUE;

            case OP_STOP_REMOTE_LISTEN:
                events.stopRemoteListen(reader.readUuid());

                return TRUE;

            case OP_LOCAL_LISTEN:
                events.localListen(localFilter(reader.readLong()), reader.readInt());

                return TRUE;

            case OP_REMOTE_QUERY_ASYNC:
                startRemoteQuery(reader, eventsAsync);

                readAndListenFuture(reader, currentFuture(), eventColResWriter);

                return TRUE;

            case OP_WAIT_FOR_LOCAL_ASYNC: {
                startWaitForLocal(reader, eventsAsync);

                readAndListenFuture(reader, currentFuture(), eventResWriter);

                return TRUE;
            }

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "ConstantConditions", "unchecked"})
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_LOCAL_QUERY: {
                Collection<EventAdapter> result =
                    events.localQuery(F.<EventAdapter>alwaysTrue(), readEventTypes(reader));

                writer.writeInt(result.size());

                for (EventAdapter e : result)
                    platformCtx.writeEvent(writer, e);

                break;
            }

            case OP_WAIT_FOR_LOCAL: {
                EventAdapter result = startWaitForLocal(reader, events);

                platformCtx.writeEvent(writer, result);

                break;
            }

            case OP_REMOTE_LISTEN: {
                int bufSize = reader.readInt();

                long interval = reader.readLong();

                boolean autoUnsubscribe = reader.readBoolean();

                boolean hasLocFilter = reader.readBoolean();

                PlatformEventFilterListener locFilter = hasLocFilter ? localFilter(reader.readLong()) : null;

                boolean hasRmtFilter = reader.readBoolean();

                UUID listenId;

                if (hasRmtFilter) {
                    PlatformEventFilterListener rmtFilter = platformCtx.createRemoteEventFilter(
                        reader.readObjectDetached(), readEventTypes(reader));

                    listenId = events.remoteListen(bufSize, interval, autoUnsubscribe, locFilter, rmtFilter);
                }
                else
                    listenId = events.remoteListen(bufSize, interval, autoUnsubscribe, locFilter, null,
                        readEventTypes(reader));

                writer.writeUuid(listenId);

                break;
            }

            case OP_REMOTE_QUERY: {
                Collection<Event> result = startRemoteQuery(reader, events);

                eventColResWriter.write(writer, result, null);

                break;
            }

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /**
     * Starts the waitForLocal.
     *
     * @param reader Reader
     * @param events Events.
     * @return Result.
     */
    private EventAdapter startWaitForLocal(BinaryRawReaderEx reader, IgniteEvents events) {
        Long filterHnd = reader.readObject();

        IgnitePredicate filter = filterHnd != null ? localFilter(filterHnd) : null;

        int[] eventTypes = readEventTypes(reader);

        return (EventAdapter) events.waitForLocal(filter, eventTypes);
    }

    /**
     * Starts the remote query.
     *
     * @param reader Reader.
     * @param events Events.
     * @return Result.
     */
    private Collection<Event> startRemoteQuery(BinaryRawReaderEx reader, IgniteEvents events) {
        Object pred = reader.readObjectDetached();

        long timeout = reader.readLong();

        int[] types = readEventTypes(reader);

        PlatformEventFilterListener filter = platformCtx.createRemoteEventFilter(pred, types);

        return events.remoteQuery(filter, timeout);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_ENABLED_EVENTS:
                writeEventTypes(events.enabledEvents(), writer);

                break;

            default:
                super.processOutStream(type, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        switch (type) {
            case OP_WITH_ASYNC:
                if (events.isAsync())
                    return this;

                return new PlatformEvents(platformCtx, events.withAsync());
        }

        return super.processOutObject(type);
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_IS_ENABLED:
                return events.isEnabled((int)val) ? TRUE : FALSE;

            case OP_STOP_LOCAL_LISTEN:
                return events.stopLocalListen(localFilter(val)) ? TRUE : FALSE;
        }

        return super.processInLongOutLong(type, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture currentFuture() throws IgniteCheckedException {
        return ((IgniteFutureImpl)eventsAsync.future()).internalFuture();
    }

    /** {@inheritDoc} */
    @Nullable @Override public PlatformFutureUtils.Writer futureWriter(int opId) {
        switch (opId) {
            case OP_WAIT_FOR_LOCAL:
                return eventResWriter;

            case OP_REMOTE_QUERY:
                return eventColResWriter;
        }

        return null;
    }

    /**
     *  Reads event types array.
     *
     * @param reader Reader
     * @return Event types, or null.
     */
    private int[] readEventTypes(BinaryRawReaderEx reader) {
        return reader.readIntArray();
    }

    /**
     *  Reads event types array.
     *
     * @param writer Writer
     * @param types Types.
     */
    private void writeEventTypes(int[] types, BinaryRawWriterEx writer) {
        if (types == null) {
            writer.writeIntArray(null);

            return;
        }

        int[] resultTypes = new int[types.length];

        int idx = 0;

        for (int t : types)
            if (platformCtx.isEventTypeSupported(t))
                resultTypes[idx++] = t;

        writer.writeIntArray(Arrays.copyOf(resultTypes, idx));
    }

    /**
     * Creates an interop filter from handle.
     *
     * @param hnd Handle.
     * @return Interop filter.
     */
    private PlatformEventFilterListener localFilter(long hnd) {
        return platformCtx.createLocalEventFilter(hnd);
    }

    /**
     * Writes an EventBase.
     */
    private static class EventResultWriter implements PlatformFutureUtils.Writer {
        /** */
        private final PlatformContext platformCtx;

        /**
         * Constructor.
         *
         * @param platformCtx Context.
         */
        public EventResultWriter(PlatformContext platformCtx) {
            assert platformCtx != null;

            this.platformCtx = platformCtx;
        }

        /** <inheritDoc /> */
        @Override public void write(BinaryRawWriterEx writer, Object obj, Throwable err) {
            platformCtx.writeEvent(writer, (EventAdapter)obj);
        }

        /** <inheritDoc /> */
        @Override public boolean canWrite(Object obj, Throwable err) {
            return obj instanceof EventAdapter && err == null;
        }
    }

    /**
     * Writes a collection of EventAdapter.
     */
    private static class EventCollectionResultWriter implements PlatformFutureUtils.Writer {
        /** */
        private final PlatformContext platformCtx;

        /**
         * Constructor.
         *
         * @param platformCtx Context.
         */
        public EventCollectionResultWriter(PlatformContext platformCtx) {
            assert platformCtx != null;

            this.platformCtx = platformCtx;
        }

        /** <inheritDoc /> */
        @SuppressWarnings("unchecked")
        @Override public void write(BinaryRawWriterEx writer, Object obj, Throwable err) {
            Collection<Event> events = (Collection<Event>)obj;

            if (obj != null) {
                writer.writeInt(events.size());

                for (Event e : events)
                    platformCtx.writeEvent(writer, e);
            }
            else {
                writer.writeInt(-1);
            }
        }

        /** <inheritDoc /> */
        @Override public boolean canWrite(Object obj, Throwable err) {
            return obj instanceof Collection && err == null;
        }
    }
}
