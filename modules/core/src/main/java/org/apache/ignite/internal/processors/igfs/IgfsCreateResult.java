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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.io.OutputStream;

/**
 * IGFS file create or append result.
 */
public class IgfsCreateResult {
    /** File info in the primary file system. */
    private final IgfsEntryInfo info;

    /** Output stream to the secondary file system. */
    private final OutputStream secondaryOut;

    /**
     * Constructor.
     *
     * @param info File info in the primary file system.
     * @param secondaryOut Output stream to the secondary file system.
     */
    public IgfsCreateResult(IgfsEntryInfo info, @Nullable OutputStream secondaryOut) {
        assert info != null;

        this.info = info;
        this.secondaryOut = secondaryOut;
    }

    /**
     * @return File info in the primary file system.
     */
    public IgfsEntryInfo info() {
        return info;
    }

    /**
     * @return Output stream to the secondary file system.
     */
    @Nullable public OutputStream secondaryOutputStream() {
        return secondaryOut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsCreateResult.class, this);
    }
}