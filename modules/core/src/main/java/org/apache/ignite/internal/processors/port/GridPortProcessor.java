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

package org.apache.ignite.internal.processors.port;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.TreeSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgnitePortProtocol;

/**
 * Registers and deregisters all ports used by SPI and Manager.
 */
public class GridPortProcessor extends GridProcessorAdapter {
    /** Collection of records about ports use. */
    private final Collection<GridPortRecord> recs;

    /** Listeners. */
    private final Collection<GridPortListener> lsnrs = new LinkedHashSet<>();

    /**
     * @param ctx Kernal context.
     */
    public GridPortProcessor(GridKernalContext ctx) {
        super(ctx);

        recs = new TreeSet<>(new Comparator<GridPortRecord>() {
            @Override public int compare(GridPortRecord o1, GridPortRecord o2) {
                int p1 = o1.port();
                int p2 = o2.port();

                return p1 < p2 ? -1 : p1 == p2 ? 0 : 1;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Started port processor.");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Stopped port processor.");
    }

    /**
     * Registers port using by passed class.
     *
     * @param port Port.
     * @param proto Protocol.
     * @param cls Class.
     */
    public void registerPort(int port, IgnitePortProtocol proto, Class cls) {
        assert port > 0 && port < 65535;
        assert proto != null;
        assert cls != null;

        synchronized (recs) {
            recs.add(new GridPortRecord(port, proto, cls));
        }

        notifyListeners();
    }

    /**
     * Deregisters all ports used by passed class.
     *
     * @param cls Class.
     */
    public void deregisterPorts(Class cls) {
        assert cls != null;

        synchronized (recs) {
            for (Iterator<GridPortRecord> iter = recs.iterator(); iter.hasNext();) {
                GridPortRecord pr = iter.next();

                if (pr.clazz().equals(cls))
                    iter.remove();
            }
        }

        notifyListeners();
    }

    /**
     * Deregisters port used by passed class.
     *
     * @param port Port.
     * @param proto Protocol.
     * @param cls Class.
     */
    public void deregisterPort(int port, IgnitePortProtocol proto, Class cls) {
        assert port > 0 && port < 65535;
        assert proto != null;
        assert cls != null;

        synchronized (recs) {
            for (Iterator<GridPortRecord> iter = recs.iterator(); iter.hasNext();) {
                GridPortRecord pr = iter.next();

                if (pr.port() == port && pr.protocol() == proto && pr.clazz().equals(cls))
                    iter.remove();
            }
        }

        notifyListeners();
    }

    /**
     * Returns unmodifiable collections of records.
     *
     * @return Unmodifiable collections of records
     */
    public Collection<GridPortRecord> records() {
        synchronized (recs) {
            return Collections.unmodifiableCollection(new ArrayList<>(recs));
        }
    }

    /**
     * Add listener.
     *
     * @param lsnr Listener.
     */
    public void addPortListener(GridPortListener lsnr) {
        assert lsnr != null;

        synchronized (lsnrs) {
            lsnrs.add(lsnr);
        }
    }

    /**
     * Remove listener.
     *
     * @param lsnr Listener.
     */
    public void removePortListener(GridPortListener lsnr) {
        assert lsnr != null;

        synchronized (lsnrs) {
            lsnrs.remove(lsnr);
        }
    }

    /**
     * Notify all listeners
     */
    private void notifyListeners() {
        synchronized (lsnrs) {
            for (GridPortListener lsnr : lsnrs)
                lsnr.onPortChange();
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        int recsSize;

        synchronized (recs) {
            recsSize = recs.size();
        }

        int lsnrsSize;

        synchronized (lsnrs) {
            lsnrsSize = lsnrs.size();
        }

        X.println(">>>");
        X.println(">>> Task session processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>  recsSize: " + recsSize);
        X.println(">>>  lsnrsSize: " + lsnrsSize);
    }
}