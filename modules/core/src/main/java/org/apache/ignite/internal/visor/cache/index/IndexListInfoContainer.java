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

package org.apache.ignite.internal.visor.cache.index;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Comparator;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Container for index info.
 */
public class IndexListInfoContainer extends IgniteDataTransferObject {
    /** Empty group name. */
    public static final String EMPTY_GROUP_NAME = "no_group";

    /** Required for serialization */
    private static final long serialVersionUID = 0L;

    /** Group name. */
    private String grpName;

    /** Cache name. */
    private String cacheName;

    /** Index name. */
    private String idxName;

    /** Columns names. */
    @GridToStringInclude
    private Collection<String> colsNames;

    /** Table name. */
    private String tblName;

    /**
     * Empty constructor required for Serializable.
     */
    public IndexListInfoContainer() {
        // No-op.
    }

    /** */
    public IndexListInfoContainer(
        GridCacheContext ctx,
        String idxName,
        Collection<String> colsNames,
        String tblName)
    {
        cacheName = ctx.name();

        final String cfgGrpName = ctx.config().getGroupName();
        grpName = cfgGrpName == null ? EMPTY_GROUP_NAME : cfgGrpName;

        this.idxName = idxName;
        this.colsNames = colsNames;
        this.tblName = tblName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        U.writeString(out, grpName);
        U.writeString(out, idxName);
        U.writeCollection(out, colsNames);
        U.writeString(out, tblName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        grpName = U.readString(in);
        idxName = U.readString(in);
        colsNames = U.readCollection(in);
        tblName = U.readString(in);
    }

    /**
     * @param tblName New table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * @return default string object representation without {@code IndexListInfoContainer} and brackets.
     */
    @Override public String toString() {
        String dfltImpl = S.toString(IndexListInfoContainer.class, this);

        return dfltImpl.substring(IndexListInfoContainer.class.getSimpleName().length() + 2,
            dfltImpl.length() - 1);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IndexListInfoContainer))
            return false;

        IndexListInfoContainer other = (IndexListInfoContainer)o;

        return cacheName.equals(other.cacheName) && grpName.equals(other.groupName()) && idxName.equals(other.idxName)
            && tblName.equals(other.tblName) && colsNames.equals(other.colsNames);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 7 * cacheName.hashCode() + 11 * grpName.hashCode() + 13 * idxName.hashCode() + 17 * colsNames.hashCode()
            + 23 * tblName.hashCode();
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Group name.
     */
    public String groupName() {
        return grpName;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Custom comparator.
     */
    public static Comparator<IndexListInfoContainer> comparator() {
        return Comparator.comparing(IndexListInfoContainer::groupName)
            .thenComparing(IndexListInfoContainer::cacheName)
            .thenComparing(IndexListInfoContainer::indexName);
    }
}
