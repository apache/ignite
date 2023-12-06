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

package org.apache.ignite.internal.management.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/** */
public class TxInfoCommandArg extends TxCommand.AbstractTxCommandArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(
        example = "<TX identifier as GridCacheVersion [topVer=..., order=..., nodeOrder=...] (can be found in logs)>|" +
            "<TX identifier as UUID (can be retrieved via --tx command)>")
    private String value;

    /** */
    private IgniteUuid uuid;

    /** */
    private GridCacheVersion gridCacheVersion;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, value);
        U.writeIgniteUuid(out, uuid);
        out.writeObject(gridCacheVersion);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        value = U.readString(in);
        uuid = U.readIgniteUuid(in);
        gridCacheVersion = (GridCacheVersion)in.readObject();
    }

    /** */
    public void uuid(IgniteUuid uuid) {
        this.uuid = uuid;
    }

    /** */
    public String value() {
        return value;
    }

    /** */
    public void value(String value) {
        this.value = value;

        try {
            uuid = IgniteUuid.fromString(value);

            return;
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

        Matcher m = p.matcher(value);

        if (!m.find()) {
            throw new IllegalArgumentException("Argument for tx --info should be " +
                "either UUID or GridCacheVersion text representation: " + value);
        }

        assert m.groupCount() == 4 : "Unexpected group count [cnt=" + m.groupCount() + ", pattern=" + regexPtrn + ']';

        try {
            gridCacheVersion = new GridCacheVersion(
                Integer.parseInt(m.group(1)),
                Integer.parseInt(m.group(3)),
                Long.parseLong(m.group(2))
            );
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Argument for tx --info should be " +
                "either UUID or GridCacheVersion text representation: " + value, e);
        }
    }

    /** */
    public GridCacheVersion gridCacheVersion() {
        return gridCacheVersion;
    }

    /** */
    public void gridCacheVersion(GridCacheVersion gridCacheVer) {
        this.gridCacheVersion = gridCacheVer;
    }

    /** */
    public IgniteUuid uuid() {
        return uuid;
    }
}
