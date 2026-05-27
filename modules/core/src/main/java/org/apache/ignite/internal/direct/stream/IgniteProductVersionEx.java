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

package org.apache.ignite.internal.direct.stream;

import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Trivial extension of {@link IgniteProductVersion} to provide setters during read in {@link DirectByteBufferStream}.
 * Don't want to expose setters directly in {@link IgniteProductVersion}, because it parts of public API.
 * Don't want to create some kind of builder to save some bytes in heap.
 */
public class IgniteProductVersionEx extends IgniteProductVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public void version(byte major, byte minor, byte maintenance) {
        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
    }

    /** */
    public void revisionTimestamp(long revTs) {
        this.revTs = revTs;
    }

    /** */
    public void revisionHash(byte[] revHash) {
        this.revHash = revHash;
    }
}
