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

package org.apache.ignite.internal.processors.platform.lifecycle;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;

/**
 * Lifecycle aware bean for interop.
 */
public class PlatformLifecycleBean implements LifecycleBean {
    /** Native gateway. */
    public PlatformCallbackGateway gate;

    /** Holder pointer. */
    public long ptr;

    /**
     * Constructor.
     */
    protected PlatformLifecycleBean() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param gate Native gateway.
     * @param ptr Holder pointer.
     */
    public PlatformLifecycleBean(PlatformCallbackGateway gate, long ptr) {
        initialize(gate, ptr);
    }

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) {
        if (gate == null)
            throw new IgniteException("Interop lifecycle bean can only be used in interop mode (did " +
                    "you start the node with native platform bootstrapper?");

        assert ptr != 0;

        // Do not send after-stop events because gate will fail due to grid being stopped.
        if (evt != LifecycleEventType.AFTER_NODE_STOP)
            gate.lifecycleEvent(ptr, evt.ordinal());
    }

    /**
     * Set pointers.
     *
     * @param gate Native gateway.
     * @param ptr Target pointer.
     */
    public void initialize(PlatformCallbackGateway gate, long ptr) {
        this.gate = gate;
        this.ptr = ptr;
    }
}