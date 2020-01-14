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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgfsModeResolver implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Maximum size of map with cached path modes. */
    private static final int MAX_PATH_CACHE = 1000;

    /** Default mode. */
    private IgfsMode dfltMode;

    /** Modes for particular paths. Ordered from longest to shortest. */
    private List<T2<IgfsPath, IgfsMode>> modes;

    /** Cached modes per path. */
    private Map<IgfsPath, IgfsMode> modesCache;

    /** Set to store parent dual paths that have primary children. */
    private Set<IgfsPath> dualParentsWithPrimaryChildren;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsModeResolver() {
        // No-op.
    }

    /**
     * Constructor
     *
     * @param dfltMode Default IGFS mode.
     * @param modes List of configured modes. The order is significant as modes are added in order of occurrence.
     * @throws IgniteCheckedException On error.
     */
    public IgfsModeResolver(IgfsMode dfltMode, @Nullable ArrayList<T2<IgfsPath, IgfsMode>> modes)
            throws IgniteCheckedException {
        assert dfltMode != null;

        this.dfltMode = dfltMode;

        dualParentsWithPrimaryChildren = new HashSet<>();

        this.modes = IgfsUtils.preparePathModes(dfltMode, modes, dualParentsWithPrimaryChildren);

        if (modes != null)
            modesCache = new GridBoundedConcurrentLinkedHashMap<>(MAX_PATH_CACHE);
    }

    /**
     * Resolves IGFS mode for the given path.
     *
     * @param path IGFS path.
     * @return IGFS mode.
     */
    public IgfsMode resolveMode(IgfsPath path) {
        assert path != null;

        if (modes == null)
            return dfltMode;
        else {
            IgfsMode mode = modesCache.get(path);

            if (mode == null) {
                for (T2<IgfsPath, IgfsMode> entry : modes) {
                    if (F.eq(path, entry.getKey()) || path.isSubDirectoryOf(entry.getKey())) {
                        // As modes ordered from most specific to least specific first mode found is ours.
                        mode = entry.getValue();

                        break;
                    }
                }

                if (mode == null)
                    mode = dfltMode;

                modesCache.put(path, mode);
            }

            return mode;
        }
    }

    /**
     * Answers if the given path has an immediate child of PRIMARY mode.
     *
     * @param path The path to query.
     * @return If the given path has an immediate child of PRIMARY mode.
     */
    public boolean hasPrimaryChild(IgfsPath path) {
        return dualParentsWithPrimaryChildren.contains(path);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, dfltMode);

        if (modes != null) {
            out.writeBoolean(true);
            out.writeInt(modes.size());

            for (T2<IgfsPath, IgfsMode> pathMode : modes) {
                assert pathMode.getKey() != null;

                pathMode.getKey().writeExternal(out);

                U.writeEnum(out, pathMode.getValue());
            }
        }
        else
            out.writeBoolean(false);

        if (!F.isEmpty(dualParentsWithPrimaryChildren)) {
            out.writeBoolean(true);
            out.writeInt(dualParentsWithPrimaryChildren.size());

            for (IgfsPath p : dualParentsWithPrimaryChildren)
                p.writeExternal(out);
        }
        else
            out.writeBoolean(false);

    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dfltMode = IgfsMode.fromOrdinal(in.readByte());

        if (in.readBoolean()) {
            int size = in.readInt();

            modes = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                IgfsPath path = IgfsUtils.readPath(in);

                modes.add(new T2<>(path, IgfsMode.fromOrdinal(in.readByte())));
            }

            modesCache = new GridBoundedConcurrentLinkedHashMap<>(MAX_PATH_CACHE);
        }

        dualParentsWithPrimaryChildren = new HashSet<>();

        if (in.readBoolean()) {
            int size = in.readInt();

            for (int i = 0; i < size; i++)
                dualParentsWithPrimaryChildren.add(IgfsUtils.readPath(in));
        }
    }
}
