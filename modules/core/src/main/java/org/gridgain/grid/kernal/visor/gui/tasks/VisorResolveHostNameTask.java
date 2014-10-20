/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.GridException;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.lang.GridBiTuple;
import org.gridgain.grid.util.GridUtils;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.S;

import java.net.InetAddress;
import java.util.*;

/**
 * Task that resolve host name for specified IP address from node.
 */
@GridInternal
public class VisorResolveHostNameTask extends VisorOneNodeTask<Void, Map<String, String>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorResolveHostNameJob job(Void arg) {
        return new VisorResolveHostNameJob(arg);
    }

    /**
     * Job that resolve host name for specified IP address.
     */
    private static class VisorResolveHostNameJob extends VisorJob<Void, Map<String, String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg List of IP address for resolve.
         */
        private VisorResolveHostNameJob(Void arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, String> run(Void arg) throws GridException {
            Map<String, String> res = new HashMap<>();

            try {
                GridBiTuple<Collection<String>, Collection<String>> addrs =
                    GridUtils.resolveLocalAddresses(InetAddress.getByName("0.0.0.0"));

                assert(addrs.get1() != null);
                assert(addrs.get2() != null);

                Iterator<String> ipIt = addrs.get1().iterator();
                Iterator<String> hostIt = addrs.get2().iterator();

                while(ipIt.hasNext() && hostIt.hasNext()) {
                    String ip = ipIt.next();

                    String hostName = hostIt.next();

                    if (!F.isEmpty(hostName) && !hostName.equals(ip))
                        res.put(ip, hostName);
                }
            }
            catch (Throwable e) {
                throw new GridException("Failed to resolve host name", e);
            }

            System.out.println("Resolving of node host name");

            for (Map.Entry<String, String> ent: res.entrySet())
                System.out.println(" IP: " + ent.getKey() + ", name: " + ent.getValue());

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorResolveHostNameJob.class, this);
        }
    }
}
