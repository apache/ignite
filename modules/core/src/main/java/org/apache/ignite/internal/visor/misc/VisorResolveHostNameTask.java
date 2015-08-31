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
                    IgniteUtils.resolveLocalAddresses(InetAddress.getByName("0.0.0.0"));

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