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

package org.apache.ignite.internal.cache.query.index;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Meta data for IndexQuery response. */
public class IndexQueryResultMeta implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Full index name. */
    private String fullIdxName;

    /** Index key settings. */
    private IndexKeyTypeSettings keyTypeSettings;

    /** Amount of qury criteria. */
    private int critSize;

    /** Index key type of PrimaryKey. */
    private int pkType;

    /** */
    public IndexQueryResultMeta() {
        // No-op.
    }

    /** */
    public IndexQueryResultMeta(String fullIdxName, IndexKeyTypeSettings keyTypeSettings, int critSize, int pkType) {
        this.fullIdxName = fullIdxName;
        this.keyTypeSettings = keyTypeSettings;
        this.critSize = critSize;
        this.pkType = pkType;
    }

    /** */
    public String fullIdxName() {
        return fullIdxName;
    }

    /** */
    public IndexKeyTypeSettings keyTypeSettings() {
        return keyTypeSettings;
    }

    /** */
    public int critSize() {
        return critSize;
    }

    /** */
    public int pkType() {
        return pkType;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, fullIdxName);
        out.writeObject(keyTypeSettings);
        out.writeInt(critSize);
        out.writeInt(pkType);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fullIdxName = U.readString(in);
        keyTypeSettings = (IndexKeyTypeSettings)in.readObject();
        critSize = in.readInt();
        pkType = in.readInt();
    }
}
