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

import java.util.*;
import org.apache.ignite.*;
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

    /** Cached children modes per path. */
    private Map<IgfsPath, Set<IgfsMode>> childrenModesCache;

    /**
     * @param dfltMode Default IGFS mode.
     * @param modes List of configured modes. The order is significant as modes are added in order of occurrence.
     */
    public IgfsModeResolver(IgfsMode dfltMode, @Nullable List<T2<IgfsPath, IgfsMode>> modes) throws IgniteCheckedException {
        assert dfltMode != null;

        this.dfltMode = dfltMode;

        // TODO: we assume that "modes" collection does not contain root folder, right?

        if (modes != null) {
            List<T2<IgfsPath, IgfsMode>> modes0 = filterModes(this.dfltMode, modes);

            this.modes = modes0;

            modesCache = new GridBoundedConcurrentLinkedHashMap<>(MAX_PATH_CACHE);
            childrenModesCache = new GridBoundedConcurrentLinkedHashMap<>(MAX_PATH_CACHE);
        }
    }

    /**
     * Checks, filters and sorts the modes.
     *
     * @param dfltMode The root mode. Must always be not null.
     * @param modes The subdirectory modes.
     * @return TODO
     * @throws IgniteCheckedException
     */
    private List<T2<IgfsPath, IgfsMode>> filterModes(IgfsMode dfltMode, @Nullable List<T2<IgfsPath, IgfsMode>> modes) throws IgniteCheckedException {
        if (modes == null)
            return null;

        Collections.sort(modes, CMP);

        Collections.reverse(modes); // Need ascending depth order for input (shallow first).

        final List<T2<IgfsPath, IgfsMode>> consistentModes = new LinkedList<>();

        // Add root as the first (the least deep):
        addIfConsistent(consistentModes, new T2<>(new IgfsPath("/"), dfltMode));

        assert consistentModes.size() == 1;

        for (T2<IgfsPath, IgfsMode> m: modes)
            addIfConsistent(consistentModes, m);

        return consistentModes;
    }

    /**
     *
     * @param consistentList NB: this list is sorted in descending order (deepest first).
     * @param pairToAdd
     * @throws IgniteCheckedException
     */
    private void addIfConsistent(List<T2<IgfsPath, IgfsMode>> consistentList, T2<IgfsPath, IgfsMode> pairToAdd) throws IgniteCheckedException {
        for (T2<IgfsPath, IgfsMode> consistent: consistentList) {
            if (startsWith(pairToAdd.getKey(), consistent.getKey())) {
                // We're adding a subdirectory to an existing root:
                if (consistent.getValue() == pairToAdd.getValue())
                    // No reason to add a subpath of the same mode, just ignoring it.
                    return;

                if (!consistent.getValue().canContain(pairToAdd.getValue()))
                    // TODO: beautify the message:
                    throw new IgniteCheckedException("Upper level directory " + consistent
                        + " cannot contain subdirectory " + pairToAdd + " due to the mode incompatibility.");

                // TODO: assert we always went through this break statement in the loop (any dir is a subdir of root):
                break; // finish
            }
        }

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
     * @param path Path.
     * @return Set of all modes that children paths could have.
     */
    public Set<IgfsMode> resolveChildrenModes(IgfsPath path) {
        assert path != null;

        if (modes == null)
            return Collections.singleton(dfltMode);
        else {
            Set<IgfsMode> children = childrenModesCache.get(path);

            if (children == null) {
                children = new HashSet<>(IgfsMode.values().length, 1.0f);

                IgfsMode pathDefault = dfltMode;

                for (T2<IgfsPath, IgfsMode> child : modes) {
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