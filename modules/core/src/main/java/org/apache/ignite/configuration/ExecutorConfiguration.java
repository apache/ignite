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

package org.apache.ignite.configuration;

import org.apache.ignite.IgniteCompute;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT;

/**
 * Custom thread pool configuration for compute tasks. See {@link IgniteCompute#withAsync()} for more information.
 */
public class ExecutorConfiguration {
    /** Thread pool name. */
    private String name;

    /** Thread pool size. */
    private int size = DFLT_PUBLIC_THREAD_CNT;

    /**
     * Default constructor.
     */
    public ExecutorConfiguration() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param name Thread pool name.
     */
    public ExecutorConfiguration(String name) {
        this.name = name;
    }

    /**
     * Copying constructor.
     *
     * @param other Instance to copy.
     */
    public ExecutorConfiguration(ExecutorConfiguration other) {
        assert other != null;

        name = other.name;
        size = other.size;
    }

    /**
     * Get thread pool name.
     * <p>
     * See {@link #setName(String)} for more information.
     *
     * @return Executor name.
     */
    public String getName() {
        return name;
    }

    /**
     * Set thread pool name. Name cannot be {@code null} and should be unique with respect to other custom executors.
     *
     * @param name Executor name.
     * @return {@code this} for chaining.
     */
    public ExecutorConfiguration setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Get thread pool size.
     * <p>
     * See {@link #setSize(int)} for more information.
     *
     * @return Thread pool size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Set thread pool size.
     * <p>
     * Defaults to {@link IgniteConfiguration#DFLT_PUBLIC_THREAD_CNT}.
     *
     * @param size Thread pool size.
     * @return {@code this} for chaining.
     */
    public ExecutorConfiguration setSize(int size) {
        this.size = size;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExecutorConfiguration.class, this);
    }
}
