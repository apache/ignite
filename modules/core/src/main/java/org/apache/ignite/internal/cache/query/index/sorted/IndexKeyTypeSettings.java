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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * List of settings that affects key types of index keys.
 */
public class IndexKeyTypeSettings implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether inlining POJO keys as hash is supported. */
    private boolean inlineObjHash = true;

    /** Whether inlining of POJO keys is supported. */
    private boolean inlineObjSupported = true;

    /** Whether optimized algorithm of String comparison is used. */
    private boolean strOptimizedCompare = true;

    /** Whether use unsigned bytes for storing byte arrays. */
    private boolean binaryUnsigned = true;

    /** */
    public boolean inlineObjHash() {
        return inlineObjHash;
    }

    /** */
    public IndexKeyTypeSettings inlineObjHash(boolean inlineObjHash) {
        this.inlineObjHash = inlineObjHash;

        return this;
    }

    /** */
    public boolean inlineObjSupported() {
        return inlineObjSupported;
    }

    /** */
    public IndexKeyTypeSettings inlineObjSupported(boolean inlineObjSupported) {
        this.inlineObjSupported = inlineObjSupported;

        return this;
    }

    /** */
    public boolean stringOptimizedCompare() {
        return strOptimizedCompare;
    }

    /** */
    public IndexKeyTypeSettings stringOptimizedCompare(boolean strOptimizedCompare) {
        this.strOptimizedCompare = strOptimizedCompare;

        return this;
    }

    /** */
    public boolean binaryUnsigned() {
        return binaryUnsigned;
    }

    /** */
    public IndexKeyTypeSettings binaryUnsigned(boolean binaryUnsigned) {
        this.binaryUnsigned = binaryUnsigned;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(inlineObjHash);
        out.writeBoolean(inlineObjSupported);
        out.writeBoolean(strOptimizedCompare);
        out.writeBoolean(binaryUnsigned);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        inlineObjHash = in.readBoolean();
        inlineObjSupported = in.readBoolean();
        strOptimizedCompare = in.readBoolean();
        binaryUnsigned = in.readBoolean();
    }
}
