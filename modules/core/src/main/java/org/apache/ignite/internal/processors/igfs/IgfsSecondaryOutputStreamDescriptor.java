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

import java.io.OutputStream;

/**
 * Descriptor of an output stream opened to the secondary file system.
 */
public class IgfsSecondaryOutputStreamDescriptor {
    /** File info in the primary file system. */
    private final IgfsEntryInfo info;

    /** Output stream to the secondary file system. */
    private final OutputStream out;

    /**
     * Constructor.
     *
     * @param info File info in the primary file system.
     * @param out Output stream to the secondary file system.
     */
    IgfsSecondaryOutputStreamDescriptor(IgfsEntryInfo info, OutputStream out) {
        assert info != null;
        assert out != null;

        this.info = info;
        this.out = out;
    }

    /**
     * @return File info in the primary file system.
     */
    IgfsEntryInfo info() {
        return info;
    }

    /**
     * @return Output stream to the secondary file system.
     */
    OutputStream out() {
        return out;
    }
}