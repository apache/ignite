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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.nio.file.Paths;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;

/**
 * Provides access to directories shared between all local nodes.
 * <pre>
 * ❯ tree
 * .                                                                            ← root (work directory, shared between all local nodes).
 * ├── db                                                                       ← db (shared between all local nodes).
 * │  ├── binary_meta                                                           ← binaryMetaRoot (shared between all local nodes).
 * │  ├── marshaller                                                            ← marshaller (shared between all local nodes).
 * └── snapshots                                                                ← snpsRoot (shared between all local nodes).
 * </pre>
 *
 * @see NodeFileTree
 */
public class SharedFileTree {
    /** Name of binary metadata folder. */
    protected static final String BINARY_METADATA_DIR = "binary_meta";

    /** Name of marshaller mappings folder. */
    public static final String MARSHALLER_DIR = "marshaller";

    /** Database default folder. */
    protected static final String DB_DIR = "db";

    /** Root(work) directory. */
    protected final File root;

    /** Path to the directory containing binary metadata. */
    protected final File binaryMetaRoot;

    /** Path to the directory containing marshaller files. */
    private final File marshaller;

    /** Path to the snapshot root directory. */
    private final File snpsRoot;

    /**
     * @param root Root directory.
     * @param snpsRoot Snapshot path.
     */
    protected SharedFileTree(File root, String snpsRoot) {
        A.notNull(root, "Root directory");

        this.root = root;
        this.snpsRoot = resolveDirectory(snpsRoot);

        String rootStr = root.getAbsolutePath();

        marshaller = Paths.get(rootStr, DB_DIR, MARSHALLER_DIR).toFile();
        binaryMetaRoot = Paths.get(rootStr, DB_DIR, BINARY_METADATA_DIR).toFile();
    }

    /**
     * @param root Root directory.
     */
    public SharedFileTree(File root) {
        this(root, DFLT_SNAPSHOT_DIRECTORY);
    }

    /**
     * @param root Root directory.
     */
    public SharedFileTree(String root) {
        this(new File(root), DFLT_SNAPSHOT_DIRECTORY);
    }

    /**
     * @param cfg Config to get {@code root} directory from.
     */
    public SharedFileTree(IgniteConfiguration cfg) {
        this(resolveRoot(cfg), cfg.getSnapshotPath());
    }

    /**
     * @return Path to the {@code root} directory.
     */
    public File root() {
        return root;
    }

    /**
     * @return Path to the {@code db} directory inside {@link #root()}.
     */
    public File db() {
        return new File(root, DB_DIR);
    }

    /**
     * @return Path to common binary metadata directory. Note, directory can contain data from several nodes.
     * Each node will create own directory inside this root.
     */
    public File binaryMetaRoot() {
        return binaryMetaRoot;
    }

    /** @return Path to marshaller directory. */
    public File marshaller() {
        return marshaller;
    }

    /** @return Path to snapshots root directory. */
    public File snapshotsRoot() {
        return snpsRoot;
    }

    /**
     * Creates {@link #binaryMetaRoot()} directory.
     * @return Created directory.
     * @see SharedFileTree#binaryMetaRoot()
     */
    public File mkdirBinaryMetaRoot() {
        return mkdir(binaryMetaRoot, "root binary metadata");
    }

    /**
     * Creates {@link #marshaller()} directory.
     * @return Created directory.
     * @see #marshaller()
     */
    public File mkdirMarshaller() {
        return mkdir(marshaller, "marshaller mappings");
    }

    /**
     * Creates {@link #snapshotsRoot()} directory.
     * @return Created directory.
     * @see #snapshotsRoot()
     */
    public File mkdirSnapshotsRoot() {
        return mkdir(snpsRoot, "snapshot work directory");
    }

    /**
     * @param f File to check.
     * @return {@code True} if argument can be binary meta root directory.
     */
    public static boolean binaryMetaRoot(File f) {
        return f.getAbsolutePath().endsWith(BINARY_METADATA_DIR);
    }

    /**
     * @param f File to check.
     * @return {@code True} if f ends with binary meta root directory.
     */
    public static boolean marshaller(File f) {
        return f.getAbsolutePath().endsWith(MARSHALLER_DIR);
    }

    /**
     * @param file File to check.
     * @return {@code True} if {@code f} contains binary meta root directory.
     */
    public static boolean containsBinaryMetaPath(File file) {
        return file.getPath().contains(BINARY_METADATA_DIR);
    }

    /**
     * @param f File to check.
     * @return {@code True} if {@code f} contains marshaller directory.
     */
    public static boolean containsMarshaller(File f) {
        return f.getAbsolutePath().contains(MARSHALLER_DIR);
    }

    /**
     * @param dir Directory to create.
     */
    public static File mkdir(File dir, String name) {
        if (!U.mkdirs(dir))
            throw new IgniteException("Could not create directory for " + name + ": " + dir);

        if (!dir.canRead())
            throw new IgniteException("Cannot read from directory: " + dir);

        if (!dir.canWrite())
            throw new IgniteException("Cannot write to directory: " + dir);

        return dir;
    }

    /**
     * @param cfg Ignite config.
     * @return Root directory.
     */
    protected static File resolveRoot(IgniteConfiguration cfg) {
        try {
            return new File(U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Creates a directory specified by the given arguments.
     *
     * @param cfg Configured directory path.
     * @return Initialized directory.
     */
    private File resolveDirectory(String cfg) {
        File sharedDir = new File(cfg);

        return sharedDir.isAbsolute()
            ? sharedDir
            : new File(root, cfg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SharedFileTree.class, this);
    }
}
