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
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public class GridGgfsModeResolver {
    /** Maximum size of map with cached path modes. */
    private static final int MAX_PATH_CACHE = 1000;

    /** Default mode. */
    private final IgniteFsMode dfltMode;

    /** Modes for particular paths. Ordered from longest to shortest. */
    private ArrayList<T2<IgniteFsPath, IgniteFsMode>> modes;

    /** Cached modes per path. */
    private Map<IgniteFsPath, IgniteFsMode> modesCache;

    /** Cached children modes per path. */
    private Map<IgniteFsPath, Set<IgniteFsMode>> childrenModesCache;

    /**
     * @param dfltMode Default GGFS mode.
     * @param modes List of configured modes.
     */
    public GridGgfsModeResolver(IgniteFsMode dfltMode, @Nullable List<T2<IgniteFsPath, IgniteFsMode>> modes) {
        assert dfltMode != null;

        this.dfltMode = dfltMode;

        if (modes != null) {
            ArrayList<T2<IgniteFsPath, IgniteFsMode>> modes0 = new ArrayList<>(modes);

            // Sort paths, longest first.
            Collections.sort(modes0, new Comparator<Map.Entry<IgniteFsPath, IgniteFsMode>>() {
                @Override public int compare(Map.Entry<IgniteFsPath, IgniteFsMode> o1,
                    Map.Entry<IgniteFsPath, IgniteFsMode> o2) {
                    return o2.getKey().components().size() - o1.getKey().components().size();
                }
            });

            this.modes = modes0;

            modesCache = new GridBoundedConcurrentLinkedHashMap<>(MAX_PATH_CACHE);
            childrenModesCache = new GridBoundedConcurrentLinkedHashMap<>(MAX_PATH_CACHE);
        }
    }

    /**
     * Resolves GGFS mode for the given path.
     *
     * @param path GGFS path.
     * @return GGFS mode.
     */
    public IgniteFsMode resolveMode(IgniteFsPath path) {
        assert path != null;

        if (modes == null)
            return dfltMode;
        else {
            IgniteFsMode mode = modesCache.get(path);

            if (mode == null) {
                for (T2<IgniteFsPath, IgniteFsMode> entry : modes) {
                    if (startsWith(path, entry.getKey())) {
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
     * @param path Path.
     * @return Set of all modes that children paths could have.
     */
    public Set<IgniteFsMode> resolveChildrenModes(IgniteFsPath path) {
        assert path != null;

        if (modes == null)
            return Collections.singleton(dfltMode);
        else {
            Set<IgniteFsMode> children = childrenModesCache.get(path);

            if (children == null) {
                children = new HashSet<>(IgniteFsMode.values().length, 1.0f);

                IgniteFsMode pathDefault = dfltMode;

                for (T2<IgniteFsPath, IgniteFsMode> child : modes) {
                    if (startsWith(path, child.getKey())) {
                        pathDefault = child.getValue();

                        break;
                    }
                    else if (startsWith(child.getKey(), path))
                        children.add(child.getValue());
                }

                children.add(pathDefault);

                childrenModesCache.put(path, children);
            }

            return children;
        }
    }

    /**
     * @return Unmodifiable copy of properly ordered modes prefixes
     *  or {@code null} if no modes set.
     */
    @Nullable public List<T2<IgniteFsPath, IgniteFsMode>> modesOrdered() {
        return modes != null ? Collections.unmodifiableList(modes) : null;
    }

    /**
     * Check if path starts with prefix.
     *
     * @param path Path.
     * @param prefix Prefix.
     * @return {@code true} if path starts with prefix, {@code false} if not.
     */
    private static boolean startsWith(IgniteFsPath path, IgniteFsPath prefix) {
        List<String> p1Comps = path.components();
        List<String> p2Comps = prefix.components();

        if (p2Comps.size() > p1Comps.size())
            return false;

        for (int i = 0; i < p1Comps.size(); i++) {
            if (i >= p2Comps.size() || p2Comps.get(i) == null)
                // All prefix components already matched.
                return true;

            if (!p1Comps.get(i).equals(p2Comps.get(i)))
                return false;
        }

        // Path and prefix components had same length and all of them matched.
        return true;
    }
}
