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

import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Exception occurred on native side.
 */
public class PlatformNativeException extends PlatformException implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Native cause. */
    protected Object cause;

    /**
     * {@link java.io.Externalizable} support.
     */
    public PlatformNativeException() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cause Native cause.
     */
    public PlatformNativeException(Object cause) {
        super("Native platform exception occurred.");

        this.cause = cause;
    }

    /**
     * @return Native cause.
     */
    public Object cause() {
        return cause;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cause);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cause = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformNativeException.class, this,
            "cause", S.INCLUDE_SENSITIVE ? cause : (cause == null ? "null" : cause.getClass().getSimpleName()));
    }
}