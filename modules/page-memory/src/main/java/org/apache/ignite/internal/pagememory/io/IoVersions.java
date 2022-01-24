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

package org.apache.ignite.internal.pagememory.io;

import org.apache.ignite.internal.tostring.S;

/**
 * Registry for IO versions of the same type.
 */
public final class IoVersions<V extends PageIo> {
    /**
     * Sorted array of IO objects.
     */
    private final V[] vers;

    /**
     * Page type.
     */
    private final int type;

    /**
     * Last element of {@link #vers} for faster access.
     */
    private final V latest;

    /**
     * Constructor.
     *
     * @param vers Array of IOs. All {@link PageIo#getType()} must match and all {@link PageIo#getVersion()} must continuously increase,
     *            starting with {@code 1}.
     */
    @SafeVarargs
    public IoVersions(V... vers) {
        assert vers != null;
        assert vers.length > 0;

        this.vers = vers;
        this.type = vers[0].getType();

        latest = vers[vers.length - 1];

        assert checkVersions();
    }

    /**
     * Returns type of {@link PageIo}s.
     */
    public int getType() {
        return type;
    }

    /**
     * Checks versions array invariants.
     *
     * @return {@code true} If versions are correct.
     */
    private boolean checkVersions() {
        for (int i = 0; i < vers.length; i++) {
            V v = vers[i];

            if (v.getType() != type || v.getVersion() != i + 1) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns latest IO version.
     */
    public V latest() {
        return latest;
    }

    /**
     * Returns IO for the given version.
     *
     * @param ver Version.
     * @return IO.
     * @throws IllegalStateException If requested version is not found.
     */
    public V forVersion(int ver) {
        if (ver == 0) {
            throw new IllegalStateException("Failed to get page IO instance (page content is corrupted)");
        }

        return vers[ver - 1];
    }

    /**
     * Returns IO for the given page.
     *
     * @param pageAddr Page address.
     * @return IO.
     * @throws IllegalStateException If requested version is not found.
     */
    public V forPage(long pageAddr) {
        int ver = PageIo.getVersion(pageAddr);

        V res = forVersion(ver);

        assert res.getType() == PageIo.getType(pageAddr) : "resType=" + res.getType() + ", pageType=" + PageIo.getType(pageAddr);

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(IoVersions.class, this);
    }
}
