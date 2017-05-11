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

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for {@link VisorScanQueryTask}.
 */
public class VisorScanQueryTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name for query. */
    private String cacheName;

    /** Filter text. */
    private String filter;

    /** Filter is regular expression */
    private boolean regEx;

    /** Case sensitive filtration */
    private boolean caseSensitive;

    /** Scan of near cache */
    private boolean near;

    /** Flag whether to execute query locally. */
    private boolean loc;

    /** Result batch size. */
    private int pageSize;

    /**
     * Default constructor.
     */
    public VisorScanQueryTaskArg() {
        // No-op.
    }

    /**
     * @param cacheName Cache name for query.
     * @param filter Filter text.
     * @param regEx Filter is regular expression.
     * @param caseSensitive Case sensitive filtration.
     * @param near Scan near cache.
     * @param loc Flag whether to execute query locally.
     * @param pageSize Result batch size.
     */
    public VisorScanQueryTaskArg(String cacheName, String filter, boolean regEx, boolean caseSensitive, boolean near,
        boolean loc, int pageSize) {
        this.cacheName = cacheName;
        this.filter = filter;
        this.regEx = regEx;
        this.caseSensitive = caseSensitive;
        this.near = near;
        this.loc = loc;
        this.pageSize = pageSize;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Filter is regular expression.
     */
    public boolean isRegEx() {
        return regEx;
    }

    /**
     * @return Filter.
     */
    public String getFilter() {
        return filter;
    }

    /**
     * @return Case sensitive filtration.
     */
    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    /**
     * @return Scan of near cache.
     */
    public boolean isNear() {
        return near;
    }

    /**
     * @return {@code true} if query should be executed locally.
     */
    public boolean isLocal() {
        return loc;
    }

    /**
     * @return Page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        U.writeString(out, filter);
        out.writeBoolean(regEx);
        out.writeBoolean(caseSensitive);
        out.writeBoolean(near);
        out.writeBoolean(loc);
        out.writeInt(pageSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        filter = U.readString(in);
        regEx = in.readBoolean();
        caseSensitive = in.readBoolean();
        near = in.readBoolean();
        loc = in.readBoolean();
        pageSize = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorScanQueryTaskArg.class, this);
    }
}
