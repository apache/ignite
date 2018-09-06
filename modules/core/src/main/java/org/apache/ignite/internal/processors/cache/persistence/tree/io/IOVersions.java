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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Registry for IO versions.
 */
public final class IOVersions<V extends PageIO> {
    /** */
    private final V[] vers;

    /** */
    private final int type;

    /** */
    private final V latest;

    /**
     * @param vers Versions.
     */
    @SafeVarargs
    public IOVersions(V... vers) {
        assert vers != null;
        assert vers.length > 0;

        this.vers = vers;
        this.type = vers[0].getType();

        latest = vers[vers.length - 1];

        assert checkVersions();
    }

    /**
     * @return Type.
     */
    public int getType() {
        return type;
    }

    /**
     * @return {@code true} If versions are correct.
     */
    private boolean checkVersions() {
        for (int i = 0; i < vers.length; i++) {
            V v = vers[i];

            if (v.getType() != type || v.getVersion() != i + 1)
                return false;
        }

        return true;
    }

    /**
     * @return Latest IO version.
     */
    public V latest() {
        return latest;
    }

    /**
     * @param ver Version.
     * @return IO.
     */
    public V forVersion(int ver) {
        if (ver == 0)
            throw new IllegalStateException("Failed to get page IO instance (page content is corrupted)");

        return vers[ver - 1];
    }

    /**
     * @param pageAddr Page address.
     * @return IO.
     */
    public V forPage(long pageAddr) {
        int ver = PageIO.getVersion(pageAddr);

        V res = forVersion(ver);

        assert res.getType() == PageIO.getType(pageAddr) : "resType=" + res.getType() +
            ", pageType=" + PageIO.getType(pageAddr);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IOVersions.class, this);
    }
}
