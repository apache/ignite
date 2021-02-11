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
package org.apache.ignite.internal.visor.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Argument for {@link FetchNearXidVersionTask}.
 */
public class TxVerboseId extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Transaction identifier (xid or nearXid) as UUID. */
    private IgniteUuid uuid;

    /** Transaction identifier (xid or nearXid) as {@link GridCacheVersion}. */
    private GridCacheVersion gridCacheVer;

    /**
     * Default constructor.
     */
    public TxVerboseId() {
        // No-op.
    }

    /**
     * @param uuid Uuid.
     * @param gridCacheVer Grid cache version.
     */
    public TxVerboseId(
        IgniteUuid uuid,
        GridCacheVersion gridCacheVer
    ) {
        this.uuid = uuid;
        this.gridCacheVer = gridCacheVer;
    }

    /**
     * Parses instance from text representation (can be UUID or GridCacheVersion).
     *
     * @param text Text representation.
     */
    public static TxVerboseId fromString(String text) {
        try {
            return new TxVerboseId(IgniteUuid.fromString(text), null);
        }
        catch (RuntimeException ignored) {
            // UUID parsing failed, let's try to parse GridCacheVersion.
        }

        GridCacheVersion gcv = new GridCacheVersion(0, 0, 0L);

        String regexPtrn = gcv.toString()
            .replaceAll("\\d+", "(\\\\d+)")
            .replaceAll("\\[", "\\\\[")
            .replaceAll("\\]", "\\\\]");

        Pattern p = Pattern.compile(regexPtrn);

        Matcher m = p.matcher(text);

        if (!m.find()) {
            throw new IllegalArgumentException("Argument for tx --info should be " +
                "either UUID or GridCacheVersion text representation: " + text);
        }

        assert m.groupCount() == 3 : "Unexpected group count [cnt=" + m.groupCount() + ", pattern=" + regexPtrn + ']';

        try {
            return new TxVerboseId(null, new GridCacheVersion(
                Integer.parseInt(m.group(1)),
                Integer.parseInt(m.group(3)),
                Long.parseLong(m.group(2))
            ));
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Argument for tx --info should be " +
                "either UUID or GridCacheVersion text representation: " + text, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeIgniteUuid(out, uuid);
        out.writeObject(gridCacheVer);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        uuid = U.readIgniteUuid(in);
        gridCacheVer = (GridCacheVersion)in.readObject();
    }

    /**
     * @return Uuid.
     */
    public IgniteUuid uuid() {
        return uuid;
    }

    /**
     * @return Grid cache version.
     */
    public GridCacheVersion gridCacheVersion() {
        return gridCacheVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxVerboseId.class, this);
    }
}
