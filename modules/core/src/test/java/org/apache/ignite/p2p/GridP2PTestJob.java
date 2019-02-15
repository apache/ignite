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

package org.apache.ignite.p2p;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;

/**
 * Job is used in the GridP2PTestTask.
 */
public class GridP2PTestJob extends ComputeJobAdapter {
    /** Injected job context. */
    @TaskSessionResource
    private ComputeTaskSession taskSes;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;


    /**
     * @param arg is argument of GridP2PTestJob.
     */
    public GridP2PTestJob(Integer arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override public Serializable execute() {
        assert taskSes != null;

        ClassLoader ldr = getClass().getClassLoader();

        if (log.isInfoEnabled())
            log.info("Executing job loaded by class loader: " + ldr.getClass().getName());

        if (argument(0) != null && ignite.configuration().getNodeId().equals(taskSes.getTaskNodeId())) {
            log.error("Remote job is executed on local node.");

            return -1;
        }

        Integer arg = argument(0);
        assert arg != null;

        // Check resource loading.
        String rsrc = "org/apache/ignite/p2p/p2p.properties";

        InputStream in = ldr.getResourceAsStream(rsrc);

        if (in == null) {
            log.error("ResourceAsStream could not be loaded: " + rsrc);

            return -2;
        }

        // Test property file load.
        byte [] bytes = new byte[20];

        try {
            in.read(bytes);
        }
        catch (IOException e) {
            log.error("Failed to read from resource stream.", e);

            return -3;
        }

        String rsrcVal = new String(bytes).trim();

        if (log.isInfoEnabled())
            log.info("Remote resource content is : " + rsrcVal);

        if (!"resource=loaded".equals(rsrcVal)) {
            log.error("Invalid loaded resource value: " + rsrcVal);

            return -4;
        }

        /* Check class properties GG-1314. */
        Class cls;

        try {
            cls = Class.forName("java.math.BigInteger");
        }
        catch (ClassNotFoundException e) {
            log.error("Mandatory class can't be loaded: [java.math.BigInteger]",e);

            return -5;
        }

        if (cls != null && cls.getPackage() == null) {
            log.error("Wrong package within class: " + cls);

            return -6;
        }

        if (getClass().getPackage() == null) {
            log.error("Wrong package within class: " + getClass());

            return -6;
        }

        return arg * 10;
    }
}