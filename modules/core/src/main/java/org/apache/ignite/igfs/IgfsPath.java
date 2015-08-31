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

package org.apache.ignite.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.io.GridFilenameUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * {@code IGFS} path to file in the file system. For example, to get information about
 * a file you would use the following code:
 * <pre name="code" class="java">
 *     IgfsPath dirPath = new IgfsPath("/my/working/dir");
 *     IgfsPath filePath = new IgfsPath(dirPath, "file.txt");
 *
 *     // Get metadata about file.
 *     IgfsFile file = igfs.info(filePath);
 * </pre>
 */
public final class IgfsPath implements Comparable<IgfsPath>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** The directory separator character. */
    private static final char SLASH_CHAR = '/';

    /** The directory separator. */
    private static final String SLASH = "/";

    /** URI representing this path. Should never change after object creation or de-serialization. */
    private String path;

    /**
     * Constructs default root path.
     */
    public IgfsPath() {
        path = SLASH;
    }

    /**
     * Constructs a path from an URI
     *
     * @param uri URI to create path from.
     */
    public IgfsPath(URI uri) {
        A.notNull(uri, "uri");

        path = normalizePath(uri.getPath());
    }

    /**
     * Constructs a path from the URI string.
     *
     * @param path URI string.
     */
    public IgfsPath(String path) {
        A.ensure(!F.isEmpty(path), "'path' is null or empty");

        this.path = normalizePath(path);
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parentPath Parent path.
     * @param childPath Child path.
     */
    public IgfsPath(IgfsPath parentPath, String childPath) {
        A.notNull(parentPath, "parentPath");

        String path = GridFilenameUtils.concat(parentPath.path, childPath);

        if (F.isEmpty(path))
            throw new IllegalArgumentException("Failed to parse path" +
                " [parent=" + parentPath + ", childPath=" + childPath + ']');

        this.path = normalizePath(path);
    }

    /**
     * Initialize path with (1) not-null, (2) normalized, (3) absolute and (4) unix-format path component.
     *
     * @param path Path.
     * @return Normalized path.
     */
    private static String normalizePath(String path) {
        assert path != null;

        String normalizedPath = GridFilenameUtils.normalizeNoEndSeparator(path, true);

        if (F.isEmpty(normalizedPath))
            throw new IllegalArgumentException("Failed to normalize path: " + path);

        if (!SLASH.equals(GridFilenameUtils.getPrefix(normalizedPath)))
            throw new IllegalArgumentException("Path should be absolute: " + path);

        assert !normalizedPath.isEmpty() : "Expects normalized path is not empty.";
        assert normalizedPath.length() == 1 || !normalizedPath.endsWith(SLASH) :
            "Expects normalized path is root or don't ends with '/' symbol.";

        return normalizedPath;
    }

    /**
     * Returns the final component of this path.
     *
     * @return The final component of this path.
     */
    public String name() {
        return GridFilenameUtils.getName(path);
    }

    /**
     * Returns a root for this path.
     *
     * @return Root for this path.
     */
    public IgfsPath root() {
        return new IgfsPath();
    }

    /**
     * Split full path on components.
     *
     * @return Path components.
     */
    public List<String> components() {
        String path = this.path;

        assert path.length() >= 1 : "Path expected to be absolute: " + path;

        // Path is short-living object, so we don't need to cache component's resolution result.
        return path.length() == 1 ? Collections.<String>emptyList() : Arrays.asList(path.substring(1).split(SLASH));
    }

    /**
     * Returns the parent of a path or {@code null} if at root.
     *
     * @return The parent of a path or {@code null} if at root.
     */
    @Nullable public IgfsPath parent() {
        String path = this.path;

        if (path.length() == 1)
            return null; // Current path is root.

        path = GridFilenameUtils.getFullPathNoEndSeparator(path);

        return new IgfsPath(path);
    }

    /**
     * Adds a suffix to the final name in the path.
     *
     * @param suffix Suffix.
     * @return Path with suffix.
     */
    public IgfsPath suffix(String suffix) {
        A.ensure(!F.isEmpty(suffix), "'suffix' is null or empty.");
        A.ensure(!suffix.contains(SLASH), "'suffix' contains file's separator '" + SLASH + "'");

        return new IgfsPath(path + suffix);
    }

    /**
     * Return the number of elements in this path.
     *
     * @return The number of elements in this path, zero depth means root directory.
     */
    public int depth() {
        final String path = this.path;
        final int size = path.length();

        assert size >= 1 && path.charAt(0) == SLASH_CHAR : "Expects absolute path: " + path;

        if (size == 1)
            return 0;

        int depth = 1;

        // Ignore the first character.
        for (int i = 1; i < size; i++)
            if (path.charAt(i) == SLASH_CHAR)
                depth++;

        return depth;
    }

    /**
     * Checks whether this path is a sub-directory of argument.
     *
     * @param path Path to check.
     * @return {@code True} if argument is same or a sub-directory of this object.
     */
    public boolean isSubDirectoryOf(IgfsPath path) {
        A.notNull(path, "path");

        return this.path.startsWith(path.path.endsWith(SLASH) ? path.path : path.path + SLASH);
    }

    /**
     * Checks if paths are identical.
     *
     * @param path Path to check.
     * @return {@code True} if paths are identical.
     */
    public boolean isSame(IgfsPath path) {
        A.notNull(path, "path");

        return this == path || this.path.equals(path.path);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(IgfsPath o) {
        return path.compareTo(o.path);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, path);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        path = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return path.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o == this || o != null && getClass() == o.getClass() && path.equals(((IgfsPath)o).path);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return path;
    }
}