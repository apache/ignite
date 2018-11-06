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

package org.apache.ignite.tensorflow.core;

import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Process manager wrapper that allows to define how one type of process specification should be transformed into
 * another type of process specification delegate working with and delegate all operations to this delegate.
 *
 * @param <T> Type of process specification delegate working with.
 * @param <R> Type of accepted process specifications.
 */
public abstract class ProcessManagerWrapper<T, R> implements ProcessManager<R> {
    /** */
    private static final long serialVersionUID = -6397225095261457524L;

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
