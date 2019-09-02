/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Arguments for {@link VisorIdleVerifyDumpTask}.
 */
public class VisorIdleVerifyDumpTaskArg extends VisorIdleVerifyTaskArg {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public VisorIdleVerifyDumpTaskArg() {
    }

    /**
     * @param caches Caches.
     * @param excludeCaches Caches to exclude.
     * @param skipZeros Skip zeros partitions.
     * @param cacheFilterEnum Cache kind.
     * @param checkCrc Check partition crc sum.
     */
    public VisorIdleVerifyDumpTaskArg(
        Set<String> caches,
        Set<String> excludeCaches,
        boolean skipZeros,
        CacheFilterEnum cacheFilterEnum,
        boolean checkCrc
    ) {
        super(caches, excludeCaches, skipZeros, cacheFilterEnum, checkCrc);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        out.writeBoolean(skipZeros());

        /**
         * Since protocol version 2 we must save class instance new fields to end of output object. It's needs for
         * support backward compatibility in extended (child) classes.
         *
         * TODO: https://issues.apache.org/jira/browse/IGNITE-10932 Will remove in 3.0
         */
        if (instanceOfCurrentClass()) {
            U.writeEnum(out, cacheFilterEnum());

            U.writeCollection(out, excludeCaches());

            out.writeBoolean(checkCrc());
        }
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        skipZeros(in.readBoolean());

        /**
         * Since protocol version 2 we must read class instance new fields from end of input object. It's needs for
         * support backward compatibility in extended (child) classes.
         *
         * TODO: https://issues.apache.org/jira/browse/IGNITE-10932 Will remove in 3.0
         */
        if (instanceOfCurrentClass()) {
            if (protoVer >= V2)
                cacheFilterEnum(CacheFilterEnum.fromOrdinal(in.readByte()));
            else
                cacheFilterEnum(CacheFilterEnum.DEFAULT);

            if (protoVer >= V2)
                excludeCaches(U.readSet(in));

            if (protoVer >= V3)
                checkCrc(in.readBoolean());
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return (byte)Math.max(V2, super.getProtocolVersion());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIdleVerifyDumpTaskArg.class, this);
    }

    /**
     * @return {@code True} if current instance is a instance of current class (not a child class) and {@code False} if
     * current instance is a instance of extented class (i.e child class).
     */
    private boolean instanceOfCurrentClass() {
        return VisorIdleVerifyDumpTaskArg.class == getClass();
    }
}
