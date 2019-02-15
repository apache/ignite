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

package org.apache.ignite.internal.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.mxbean.WorkersControlMXBean;

/**
 * MBean that provides control of system workersRegistry.
 */
public class WorkersControlMXBeanImpl implements WorkersControlMXBean {
    /** System worker registry. */
    private final WorkersRegistry workerRegistry;

    /**
     * Constructor.
     *
     * @param registry System worker registry.
     */
    public WorkersControlMXBeanImpl(WorkersRegistry registry) {
        workerRegistry = registry;
    }

    /** {@inheritDoc} */
    @Override public List<String> getWorkerNames() {
        List<String> names = new ArrayList<>(workerRegistry.names());

        Collections.sort(names);

        return names;
    }

    /** {@inheritDoc} */
    @Override public boolean terminateWorker(String name) {
        GridWorker w = workerRegistry.worker(name);

        if (w == null || w.isCancelled())
            return false;

        Thread t = w.runner();

        if (t == null)
            return false;

        t.interrupt();

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopThreadByUniqueName(String name) {
        Thread[] threads = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> Objects.equals(t.getName(), name))
            .toArray(Thread[]::new);

        if (threads.length != 1)
            return false;

        threads[0].stop();

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopThreadById(long id) {
        Thread[] threads = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.getId() == id)
            .toArray(Thread[]::new);

        if (threads.length != 1)
            return false;

        threads[0].stop();

        return true;
    }
}
