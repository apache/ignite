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

package org.apache.ignite.internal.processors.platform.lifecycle;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.platform.callback.*;
import org.apache.ignite.lifecycle.*;

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
