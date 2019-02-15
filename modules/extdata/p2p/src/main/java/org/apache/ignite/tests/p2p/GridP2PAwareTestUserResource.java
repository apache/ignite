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

package org.apache.ignite.tests.p2p;

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * User resource, that increases node-local counters
 * on deploy and undeploy.
 */
public class GridP2PAwareTestUserResource {
    /** Deploy counter key. */
    private static final String DEPLOY_CNT_KEY = "deployCnt";

    /** Undeploy counter key. */
    private static final String UNDEPLOY_CNT_KEY = "undeployCnt";

    /** Grid instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Concurrently increments numeric cache value.
     *
     * @param key Key for the value to be incremented.
     */
    private <T> void concurrentIncrement(T key) {
        ConcurrentMap<T, Integer> nodeLoc = ignite.cluster().nodeLocalMap();

        Integer cntr = nodeLoc.get(key);

        if (cntr == null)
            cntr = nodeLoc.putIfAbsent(key, 1);

        if (cntr != null) {
            while (!nodeLoc.replace(key, cntr, cntr + 1)) {
                cntr = nodeLoc.get(key);

                assert cntr != null;
            }
        }
    }
}