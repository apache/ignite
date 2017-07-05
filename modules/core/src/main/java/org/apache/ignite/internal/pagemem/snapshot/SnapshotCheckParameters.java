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

package org.apache.ignite.internal.pagemem.snapshot;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Tuple for passing optional parameters of {@link SnapshotOperationType#CHECK}.
 */
public class SnapshotCheckParameters implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Optional paths. */
    private final Collection<File> optionalPaths;

    /** Flag for skipping CRC check. */
    private final boolean skipCrc;

    /**
     * Factory method.
     *
     * @return Tuple with optional parameters or null if parameters are default.
     *
     * @param optionalPaths Optional paths.
     * @param skipCrc Skip crc.
     */
    @Nullable public static SnapshotCheckParameters valueOf(Collection<File> optionalPaths, boolean skipCrc) {
        if (optionalPaths == null && !skipCrc)
            return null;

        return new SnapshotCheckParameters(optionalPaths, skipCrc);
    }

    /**
     * @param optionalPaths Optional paths.
     * @param skipCrc Flag for skipping CRC check.
     */
    private SnapshotCheckParameters(Collection<File> optionalPaths, boolean skipCrc) {
        this.optionalPaths = optionalPaths;
        this.skipCrc = skipCrc;
    }

    /**
     * @return Optional paths.
     */
    public Collection<File> optionalPaths() {
        return optionalPaths;
    }

    /**
     * @return Flag for skipping CRC check.
     */
    public boolean skipCrc() {
        return skipCrc;
    }
}
