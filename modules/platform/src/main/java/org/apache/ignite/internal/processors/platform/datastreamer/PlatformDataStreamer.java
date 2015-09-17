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
import org.apache.ignite.internal.portable.PortableRawReaderEx;
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

    /** Cache name. */
    private final String cacheName;

    /** Data streamer. */
    private final DataStreamerImpl ldr;

    /** Portable flag. */
    private final boolean keepPortable;

    /** Topology update event listener. */
    private volatile GridLocalEventListener lsnr;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param ldr Data streamer.
     */
    public PlatformDataStreamer(PlatformContext platformCtx, String cacheName, DataStreamerImpl ldr,
        boolean keepPortable) {
        super(platformCtx);

        this.cacheName = cacheName;
        this.ldr = ldr;
        this.keepPortable = keepPortable;
    }

    /** {@inheritDoc}  */
    @Override protected long processInStreamOutLong(int type, PortableRawReaderEx reader) throws IgniteCheckedException {
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

            case OP_RECEIVER:
                long ptr = reader.readLong();

                Object rec = reader.readObjectDetached();

                ldr.receiver(platformCtx.createStreamReceiver(rec, ptr, keepPortable));

                return TRUE;

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /**
     * Listen topology changes.
     *
     * @param ptr Pointer.
     */
    public void listenTopology(final long ptr) {
        lsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                long topVer = discoEvt.topologyVersion();
                int topSize = platformCtx.kernalContext().discovery().cacheNodes(
                    cacheName, new AffinityTopologyVersion(topVer)).size();

                platformCtx.gateway().dataStreamerTopologyUpdate(ptr, topVer, topSize);
            }
        };

        platformCtx.kernalContext().event().addLocalEventListener(lsnr, EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT);

        GridDiscoveryManager discoMgr = platformCtx.kernalContext().discovery();

        long topVer = discoMgr.topologyVersion();
        int topSize = discoMgr.cacheNodes(cacheName, new AffinityTopologyVersion(topVer)).size();

        platformCtx.gateway().dataStreamerTopologyUpdate(ptr, topVer, topSize);
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