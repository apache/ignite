/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Base interop predicate. Delegates apply to native platform.
 */
public abstract class PlatformAbstractPredicate implements Externalizable {
    /** .Net binary predicate */
    protected Object pred;

    /** Pointer to deployed predicate. */
    protected transient long ptr;

    /** Interop processor. */
    protected transient PlatformContext ctx;

    /**
     * {@link java.io.Externalizable} support.
     */
    public PlatformAbstractPredicate() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param pred .Net binary predicate.
     * @param ptr Pointer to predicate in the native platform.
     * @param ctx Kernal context.
     */
    protected PlatformAbstractPredicate(Object pred, long ptr, PlatformContext ctx) {
        this.pred = pred;
        this.ptr = ptr;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(pred);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        pred = in.readObject();
    }
}
