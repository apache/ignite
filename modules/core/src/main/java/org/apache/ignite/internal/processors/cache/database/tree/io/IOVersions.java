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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;

/**
 * Registry for IO versions.
 */
public final class IOVersions<V extends PageIO> {
    /** */
    private final V[] vers;

    /** */
    private final int type;

    /**
     * @param vers Versions.
     */
    @SafeVarargs
    public IOVersions(V... vers) {
        assert vers != null;
        assert vers.length > 0;

        this.vers = vers;
        this.type = vers[0].getType();

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
        return forVersion(vers.length);
    }

    /**
     * @param ver Version.
     * @return IO.
     */
    public V forVersion(int ver) {
        return vers[ver - 1];
    }

    /**
     * @param buf Buffer.
     * @return IO.
     */
    public V forPage(ByteBuffer buf) {
        int ver = PageIO.getVersion(buf);

        V res = forVersion(ver);

        assert res.getType() == PageIO.getType(buf);

        return res;
    }
}
