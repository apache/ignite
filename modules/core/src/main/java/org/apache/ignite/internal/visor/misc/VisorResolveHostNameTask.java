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

package org.apache.ignite.internal.visor.misc;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Task that resolve host name for specified IP address from node.
 */
@GridInternal
public class VisorResolveHostNameTask extends VisorOneNodeTask<Void, Map<String, String>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorResolveHostNameJob job(Void arg) {
        return new VisorResolveHostNameJob(arg, debug);
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
         * @param debug Debug flag.
         */
        private VisorResolveHostNameJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, String> run(Void arg) {
            Map<String, String> res = new HashMap<>();

            try {
                IgniteBiTuple<Collection<String>, Collection<String>> addrs =
                    IgniteUtils.resolveLocalAddresses(InetAddress.getByName("0.0.0.0"), true);

                assert (addrs.get1() != null);
                assert (addrs.get2() != null);

                Iterator<String> ipIt = addrs.get1().iterator();
                Iterator<String> hostIt = addrs.get2().iterator();

                while (ipIt.hasNext() && hostIt.hasNext()) {
                    String ip = ipIt.next();

                    String hostName = hostIt.next();

                    if (hostName == null || hostName.trim().isEmpty()) {
                        try {
                            if (InetAddress.getByName(ip).isLoopbackAddress())
                                res.put(ip, "localhost");
                        }
                        catch (Exception ignore) {
                            //no-op
                        }
                    }
                    else if (!hostName.equals(ip))
                        res.put(ip, hostName);
                }
            }
            catch (Exception e) {
                throw new IgniteException("Failed to resolve host name", e);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorResolveHostNameJob.class, this);
        }
    }
}
