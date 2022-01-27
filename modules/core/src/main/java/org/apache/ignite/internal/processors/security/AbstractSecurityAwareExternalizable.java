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

package org.apache.ignite.internal.processors.security;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.AccessControlException;
import java.util.UUID;
import org.apache.ignite.internal.GridInternalWrapper;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.util.Objects.requireNonNull;

/**
 * Abstract security aware Externalizable.
 */
public abstract class AbstractSecurityAwareExternalizable<T> implements Externalizable, GridInternalWrapper<T> {
    /** Security subject id. */
    protected UUID subjectId;

    /** Original component. */
    protected T original;

    /** Ignite. */
    @IgniteInstanceResource
    protected transient IgniteEx ignite;

    /**
     * Default constructor.
     */
    protected AbstractSecurityAwareExternalizable() {
        // No-op.
    }

    /**
     * @param subjectId Security subject id.
     * @param original Original component.
     */
    protected AbstractSecurityAwareExternalizable(UUID subjectId, T original) {
        this.subjectId = requireNonNull(subjectId, "Parameter 'subjectId' cannot be null.");
        this.original = requireNonNull(original, "Parameter 'original' cannot be null.");
    }

    /**
     * Writes access denied message.
     *
     * @param e Exception to log.
     */
    protected void logAccessDeniedMessage(AccessControlException e) {
        ignite.context().log(getClass()).error("The operation can't be executed because the current subject " +
            "doesn't have appropriate permission [subjectId=" + subjectId + "].", e);
    }

    /** {@inheritDoc} */
    @Override public T userObject() {
        return original;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, subjectId);

        out.writeObject(original);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        subjectId = U.readUuid(in);

        original = (T)in.readObject();
    }
}
