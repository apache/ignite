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
import org.gridgain.grid.kernal.visor.cmd.VisorJob;
import org.gridgain.grid.kernal.visor.cmd.VisorOneNodeTask;
import org.gridgain.grid.lang.GridBiTuple;
import org.gridgain.grid.util.GridUtils;
import org.gridgain.grid.util.typedef.internal.S;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

/**
 * Task that resolve host name for specified IP address from node.
 */
@GridInternal
public class VisorResolveHostNameTask extends VisorOneNodeTask<List<String>, Set<GridBiTuple<String, String>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorResolveHostNameJob job(List<String> arg) {
        return new VisorResolveHostNameJob(arg);
    }

    /**
     * Job that resolve host name for specified IP address.
     */
    private static class VisorResolveHostNameJob extends VisorJob<List<String>, Set<GridBiTuple<String, String>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg List of IP address for resolve.
         */
        private VisorResolveHostNameJob(List<String> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Set<GridBiTuple<String, String>> run(List<String> arg) throws GridException {
            Set<GridBiTuple<String, String>> res = new HashSet<>();

            try {
                for (String addr: arg) {
                    GridBiTuple<Collection<String>, Collection<String>> tmp =
                        GridUtils.resolveLocalAddresses(InetAddress.getByName(addr));

                    Iterator<String> ipIt = tmp.get1().iterator();
                    Iterator<String> hostIt = tmp.get2().iterator();

                    while(ipIt.hasNext())
                        res.add(new GridBiTuple<>(ipIt.next(), hostIt.next()));
                }
            }
            catch (Throwable e) {
                throw new GridException("Failed to resolve host name", e);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorResolveHostNameJob.class, this);
        }
    }
}
