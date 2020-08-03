/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.verify;

import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;

/**
 * This exception is used to collect exceptions occured in {@link VerifyBackupPartitionsTaskV2} execution.
 */
public class IdleVerifyException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Occured exceptions. */
    private final Collection<IgniteException> exceptions;

    /** */
    public IdleVerifyException(Collection<IgniteException> exceptions) {
        if (F.isEmpty(exceptions))
            throw new IllegalArgumentException("Exceptions can't be empty!");

        this.exceptions = exceptions;
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return exceptions.stream()
            .map(Throwable::getMessage)
            .collect(Collectors.joining(", "));
    }

    /**
     * @return Exceptions.
     */
    public Collection<IgniteException> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
