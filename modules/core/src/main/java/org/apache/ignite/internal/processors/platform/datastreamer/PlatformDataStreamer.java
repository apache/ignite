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

package org.apache.ignite.internal.processors.platform.datastreamer;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.util.lang.GridMapEntry;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Interop data streamer wrapper.
 */
@SuppressWarnings({"UnusedDeclaration", "unchecked"})
public class PlatformDataStreamer extends PlatformAbstractTarget {
    /** Policy: continue. */
    private static final int PLC_CONTINUE = 0;

    /** Policy: close. */
    private static final int PLC_CLOSE = 1;

    /** Policy: cancel and close. */
    private static final int PLC_CANCEL_CLOSE = 2;

    /** Policy: do flush. */
    private static final int PLC_FLUSH = 3;

    /** */
    private static final int OP_UPDATE = 1;

    /** */
    private static final int OP_RECEIVER = 2;

    /** */
    private static final int OP_ALLOW_OVERWRITE = 3;

    /** */
    private static final int OP_SET_ALLOW_OVERWRITE = 4;

    /** */
    private static final int OP_SKIP_STORE = 5;

    /** */
    private static final int OP_SET_SKIP_STORE = 6;

    /** */
    private static final int OP_PER_NODE_BUFFER_SIZE = 7;

    /** */
    private static final int OP_SET_PER_NODE_BUFFER_SIZE = 8;

    /** */
    private static final int OP_PER_NODE_PARALLEL_OPS = 9;

    /** */
    private static final int OP_SET_PER_NODE_PARALLEL_OPS = 10;

    /** */
    private static final int OP_LISTEN_TOPOLOGY = 11;

    /** Cache name. */
    private final String cacheName;

    /** Data streamer. */
    private final DataStreamerImpl ldr;

    /** Binary flag. */
    private final boolean keepBinary;

    /** Topology update event listener. */
    private volatile GridLocalEventListener lsnr;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param ldr Data streamer.
     */
    public PlatformDataStreamer(PlatformContext platformCtx, String cacheName, DataStreamerImpl ldr,
        boolean keepBinary) {
        super(platformCtx);

        this.cacheName = cacheName;
        this.ldr = ldr;
        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc}  */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_UPDATE:
                int plc = reader.readInt();

                if (plc == PLC_CANCEL_CLOSE) {
                    // Close with cancel.
                    platformCtx.kernalContext().event().removeLocalEventListener(lsnr);

                    ldr.close(true);
                }
                else {
                    final long futPtr = reader.readLong();

                    int valsCnt = reader.readInt();

                    if (valsCnt > 0) {
                        Collection<GridMapEntry> vals = new ArrayList<>(valsCnt);

                        for (int i = 0; i < valsCnt; i++)
                            vals.add(new GridMapEntry(reader.readObjectDetached(), reader.readObjectDetached()));

                        PlatformFutureUtils.listen(platformCtx, ldr.addData(vals), futPtr,
                            PlatformFutureUtils.TYP_OBJ, this);
                    }

                    if (plc == PLC_CLOSE) {
                        platformCtx.kernalContext().event().removeLocalEventListener(lsnr);

                        ldr.close(false);
                    }
                    else if (plc == PLC_FLUSH)
                        ldr.tryFlush();
                    else
                        assert plc == PLC_CONTINUE;
                }

                return TRUE;

            case OP_RECEIVER: {
                long ptr = reader.readLong();

                Object rec = reader.readObjectDetached();

                ldr.receiver(platformCtx.createStreamReceiver(rec, ptr, keepBinary));

                return TRUE;
            }

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /** {@inheritDoc}  */
    @Override public long processInLongOutLong(int type, final long val) throws IgniteCheckedException {
        switch (type) {
            case OP_SET_ALLOW_OVERWRITE:
                ldr.allowOverwrite(val == TRUE);

                return TRUE;

            case OP_SET_PER_NODE_BUFFER_SIZE:
                ldr.perNodeBufferSize((int) val);

                return TRUE;

            case OP_SET_SKIP_STORE:
                ldr.skipStore(val == TRUE);

                return TRUE;

            case OP_SET_PER_NODE_PARALLEL_OPS:
                ldr.perNodeParallelOperations((int) val);

                return TRUE;

            case OP_LISTEN_TOPOLOGY: {
                lsnr = new GridLocalEventListener() {
                    @Override public void onEvent(Event evt) {
                        DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                        long topVer = discoEvt.topologyVersion();
                        int topSize = platformCtx.kernalContext().discovery().cacheNodes(
                                cacheName, new AffinityTopologyVersion(topVer)).size();

                        platformCtx.gateway().dataStreamerTopologyUpdate(val, topVer, topSize);
                    }
                };

                platformCtx.kernalContext().event().addLocalEventListener(lsnr,
                        EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT);

                GridDiscoveryManager discoMgr = platformCtx.kernalContext().discovery();

                AffinityTopologyVersion topVer = discoMgr.topologyVersionEx();

                int topSize = discoMgr.cacheNodes(cacheName, topVer).size();

                platformCtx.gateway().dataStreamerTopologyUpdate(val, topVer.topologyVersion(), topSize);

                return TRUE;
            }

            case OP_ALLOW_OVERWRITE:
                return ldr.allowOverwrite() ? TRUE : FALSE;

            case OP_PER_NODE_BUFFER_SIZE:
                return ldr.perNodeBufferSize();

            case OP_SKIP_STORE:
                return ldr.skipStore() ? TRUE : FALSE;

            case OP_PER_NODE_PARALLEL_OPS:
                return ldr.perNodeParallelOperations();
        }

        return super.processInLongOutLong(type, val);
    }

    /**
     * @return Allow-overwrite flag.
     */
    public boolean allowOverwrite() {
        return ldr.allowOverwrite();
    }

    /**
     * @param val Allow-overwrite flag.
     */
    public void allowOverwrite(boolean val) {
        ldr.allowOverwrite(val);
    }

    /**
     * @return Skip store flag.
     */
    public boolean skipStore() {
        return ldr.skipStore();
    }

    /**
     * @param skipStore Skip store flag.
     */
    public void skipStore(boolean skipStore) {
        ldr.skipStore(skipStore);
    }

    /**
     * @return Per-node buffer size.
     */
    public int perNodeBufferSize() {
        return ldr.perNodeBufferSize();
    }

    /**
     * @param val Per-node buffer size.
     */
    public void perNodeBufferSize(int val) {
        ldr.perNodeBufferSize(val);
    }

    /**
     * @return Per-node parallel load operations.
     */
    public int perNodeParallelOperations() {
        return ldr.perNodeParallelOperations();
    }

    /**
     * @param val Per-node parallel load operations.
     */
    public void perNodeParallelOperations(int val) {
        ldr.perNodeParallelOperations(val);
    }
}
