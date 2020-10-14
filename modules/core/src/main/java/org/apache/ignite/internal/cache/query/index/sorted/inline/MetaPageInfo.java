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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.lang.IgniteProductVersion;

public class MetaPageInfo {
    /** */
    int inlineSize;

    /** */
    boolean useUnwrappedPk;

    /** */
    boolean flagsSupported;

    /** */
    boolean inlineObjSupported;

    /** */
    boolean inlineObjHash;

    /** */
    IgniteProductVersion createdVer;

    /**
     * @param io Metapage IO.
     * @param pageAddr Page address.
     */
    public MetaPageInfo(BPlusMetaIO io, long pageAddr) {
        inlineSize = io.getInlineSize(pageAddr);
        useUnwrappedPk = io.unwrappedPk(pageAddr);
        flagsSupported = io.supportFlags();

        if (flagsSupported) {
            inlineObjSupported = io.inlineObjectSupported(pageAddr);
            inlineObjHash = io.inlineObjectHash(pageAddr);
        }

        createdVer = io.createdVersion(pageAddr);
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @return {@code true} In case use unwrapped PK for indexes.
     */
    public boolean useUnwrappedPk() {
        return useUnwrappedPk;
    }

    /**
     * @return {@code true} In case metapage contains flags.
     */
    public boolean flagsSupported() {
        return flagsSupported;
    }

    /**
     * @return {@code true} In case inline object is supported.
     */
    public boolean inlineObjectSupported() {
        return inlineObjSupported;
    }

    /**
     * @return {@code true} In case inline object is supported.
     */
    public boolean inlineObjectHash() {
        return inlineObjHash;
    }
}
