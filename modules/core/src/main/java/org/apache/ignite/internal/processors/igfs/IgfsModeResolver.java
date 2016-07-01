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

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    /** Depth-based comparator. Longest paths go first. */
    private static final Comparator<Map.Entry<IgfsPath, IgfsMode>> CMP
        = new Comparator<Map.Entry<IgfsPath, IgfsMode>>() {
        @Override public int compare(Map.Entry<IgfsPath, IgfsMode> o1,
            Map.Entry<IgfsPath, IgfsMode> o2) {
            return o2.getKey().components().size() - o1.getKey().components().size();
        }
    };

    /** Default mode. */
    private final IgfsMode dfltMode;

    /** Modes for particular paths. Ordered from longest to shortest. */
    private List<T2<IgfsPath, IgfsMode>> modes;

    /** Cached modes per path. */
    private Map<IgfsPath, IgfsMode> modesCache;

    /**
     * Constructor
     *
     * @param dfltMode Default IGFS mode.
     * @param modes List of configured modes. The order is significant as modes are added in order of occurrence.
     * @throws IgniteCheckedException On error.
     */
    public IgfsModeResolver(IgfsMode dfltMode, @Nullable List<T2<IgfsPath, IgfsMode>> modes)
        throws IgniteCheckedException {
        assert dfltMode != null;

        this.dfltMode = dfltMode;

        if (modes != null) {
            this.modes = filterModes(this.dfltMode, modes);

            modesCache = new GridBoundedConcurrentLinkedHashMap<>(MAX_PATH_CACHE);
        }
    }

    /**
     * Checks, filters and sorts the modes.
     *
     * @param dfltMode The root mode. Must always be not null.
     * @param modes The subdirectory modes.
     * @return Descending list of filtered and checked modes.
     * @throws IgniteCheckedException On error or
     */
    private List<T2<IgfsPath, IgfsMode>> filterModes(IgfsMode dfltMode, @Nullable List<T2<IgfsPath, IgfsMode>> modes)
        throws IgniteCheckedException {
        if (modes == null)
            return null;

        Collections.sort(modes, CMP);

        Collections.reverse(modes); // Need ascending depth order for input (shallow first).

        final List<T2<IgfsPath, IgfsMode>> consistentModes = new LinkedList<>();

        // Add root with default mode as the first (the least deep):
        final T2<IgfsPath, IgfsMode> root = new T2<>(new IgfsPath("/"), dfltMode);
        addIfConsistent(consistentModes, root);

        assert consistentModes.size() == 1;

        for (T2<IgfsPath, IgfsMode> m: modes)
            addIfConsistent(consistentModes, m);

        // Remove root, because this class contract is that root mode is not contained in the list.
        T2<IgfsPath, IgfsMode> expRoot = consistentModes.remove(consistentModes.size() - 1);

        assert expRoot == root;

        return consistentModes;
    }

    /**
     * Adds mode pair.
     *
     * @param consistentList List sorted in descending order (deepest first).
     * @param pairToAdd The pairs come in ascending order. The incoming piar is deeper than any element of
     * {@code consistentList}.
     * @throws IgniteCheckedException If such pair cannot be added.
     */
    private void addIfConsistent(List<T2<IgfsPath, IgfsMode>> consistentList, T2<IgfsPath, IgfsMode> pairToAdd)
        throws IgniteCheckedException {
        assert pairToAdd.getValue() != null;

        boolean parentFound = consistentList.isEmpty();

        for (T2<IgfsPath, IgfsMode> consistent: consistentList) {
            if (startsWith(pairToAdd.getKey(), consistent.getKey())) {
                assert consistent.getValue() != null;

                // We're adding a subdirectory to an existing root:
                if (consistent.getValue() == pairToAdd.getValue())
                    // No reason to add a sub-path of the same mode, ignoring this pair.
                    return;

                if (!consistent.getValue().canContain(pairToAdd.getValue()))
                    throw new IgniteCheckedException("Subdirectory " + pairToAdd.getKey() + " mode "
                        + pairToAdd.getValue() + " is not compatible with upper level "
                        + consistent.getKey() + " directory mode " + consistent.getValue() + ".");

                parentFound = true;

                break;
            }
        }

        assert parentFound;

        // Add to the 1st position (depest first):
        consistentList.add(0, pairToAdd);
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
     * @return Unmodifiable copy of properly ordered modes prefixes
     *  or {@code null} if no modes set.
     */
    @Nullable public List<T2<IgfsPath, IgfsMode>> modesOrdered() {
        return modes != null ? Collections.unmodifiableList(modes) : null;
    }

    /**
     * Check if path starts with prefix.
     *
     * @param path Path.
     * @param prefix Prefix.
     * @return {@code true} if path starts with prefix, {@code false} if not.
     */
    private static boolean startsWith(IgfsPath path, IgfsPath prefix) {
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