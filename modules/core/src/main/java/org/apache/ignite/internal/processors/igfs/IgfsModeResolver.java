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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgfsModeResolver {
    /** Maximum size of map with cached path modes. */
    private static final int MAX_PATH_CACHE = 1000;

    /** Default mode. */
    private final IgfsMode dfltMode;

    /** Modes for particular paths. Ordered from longest to shortest. */
    private List<T2<IgfsPath, IgfsMode>> modes;

    /** Cached modes per path. */
    private Map<IgfsPath, IgfsMode> modesCache;

    /** Set to store parent dual paths that have primary children. */
    private final Set<IgfsPath> dualParentsWithPrimaryChildren;

    /**
     * Constructor
     *
     * @param dfltMode Default IGFS mode.
     * @param modes List of configured modes. The order is significant as modes are added in order of occurrence.
     */
    public IgfsModeResolver(IgfsMode dfltMode, @Nullable ArrayList<T2<IgfsPath, IgfsMode>> modes)
            throws IgniteCheckedException {
        assert dfltMode != null;

        this.dfltMode = dfltMode;

        this.dualParentsWithPrimaryChildren = new HashSet<>();

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
                    if (path.isSame(entry.getKey()) || path.isSubDirectoryOf(entry.getKey())) {
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
     * @return Copy of properly ordered modes prefixes
     *  or {@code null} if no modes set.
     */
    @Nullable public ArrayList<T2<IgfsPath, IgfsMode>> modesOrdered() {
        return modes != null ? new ArrayList<>(modes) : null;
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
}