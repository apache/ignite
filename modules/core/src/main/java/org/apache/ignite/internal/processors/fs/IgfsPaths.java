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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.ignitefs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Description of path modes.
 */
public class IgfsPaths implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Additional secondary file system properties. */
    private Map<String, String> props;

    /** Default GGFS mode. */
    private IgniteFsMode dfltMode;

    /** Path modes. */
    private List<T2<IgniteFsPath, IgniteFsMode>> pathModes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsPaths() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param props Additional secondary file system properties.
     * @param dfltMode Default GGFS mode.
     * @param pathModes Path modes.
     */
    public IgfsPaths(Map<String, String> props, IgniteFsMode dfltMode, @Nullable List<T2<IgniteFsPath,
        IgniteFsMode>> pathModes) {
        this.props = props;
        this.dfltMode = dfltMode;
        this.pathModes = pathModes;
    }

    /**
     * @return Secondary file system properties.
     */
    public Map<String, String> properties() {
        return props;
    }

    /**
     * @return Default GGFS mode.
     */
    public IgniteFsMode defaultMode() {
        return dfltMode;
    }

    /**
     * @return Path modes.
     */
    @Nullable public List<T2<IgniteFsPath, IgniteFsMode>> pathModes() {
        return pathModes;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeStringMap(out, props);
        U.writeEnum(out, dfltMode);

        if (pathModes != null) {
            out.writeBoolean(true);
            out.writeInt(pathModes.size());

            for (T2<IgniteFsPath, IgniteFsMode> pathMode : pathModes) {
                pathMode.getKey().writeExternal(out);
                U.writeEnum(out, pathMode.getValue());
            }
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        props = U.readStringMap(in);
        dfltMode = IgniteFsMode.fromOrdinal(in.readByte());

        if (in.readBoolean()) {
            int size = in.readInt();

            pathModes = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                IgniteFsPath path = new IgniteFsPath();
                path.readExternal(in);

                T2<IgniteFsPath, IgniteFsMode> entry = new T2<>(path, IgniteFsMode.fromOrdinal(in.readByte()));

                pathModes.add(entry);
            }
        }
    }
}
