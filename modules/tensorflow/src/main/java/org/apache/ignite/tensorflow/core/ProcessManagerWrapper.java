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

package org.apache.ignite.tensorflow.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;

/**
 * Process manager wrapper that allows to define how one type of process specification should be transformed into
 * another type of process specification delegate working with and delegate all operations to this delegate.
 *
 * @param <T> Type of process specification delegate working with.
 * @param <R> Type of accepted process specifications.
 */
public abstract class ProcessManagerWrapper<T, R> implements ProcessManager<R> {
    /** Delegate. */
    private final ProcessManager<T> delegate;

    /**
     * Constructs a new instance of process manager wrapper.
     *
     * @param delegate Delegate.
     */
    public ProcessManagerWrapper(ProcessManager<T> delegate) {
        assert delegate != null : "Delegate should not be null";

        this.delegate = delegate;
    }

    /**
     * Transforms accepted process specification into process specification delegate working with.
     *
     * @param spec Accepted process specification.
     * @return Process specification delegate working with.
     */
    protected abstract T transformSpecification(R spec);

    /** {@inheritDoc} */
    @Override public Map<UUID, List<UUID>> start(List<R> specifications) {
        List<T> transformedSpecifications = new ArrayList<>();

        for (R spec : specifications)
            transformedSpecifications.add(transformSpecification(spec));

        return delegate.start(transformedSpecifications);
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<LongRunningProcessStatus>> ping(Map<UUID, List<UUID>> procIds) {
        return delegate.ping(procIds);
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<LongRunningProcessStatus>> stop(Map<UUID, List<UUID>> procIds, boolean clear) {
        return delegate.stop(procIds, clear);
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<LongRunningProcessStatus>> clear(Map<UUID, List<UUID>> procIds) {
        return delegate.clear(procIds);
    }
}
