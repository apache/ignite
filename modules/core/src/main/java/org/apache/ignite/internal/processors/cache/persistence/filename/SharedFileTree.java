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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;

/**
 * Provides access to directories shared between all nodes.
 *
 * ❯ tree
 * .                                                                            ← root (work directory, shared between all nodes).
 * ├── db                                                                       ← db (shared between all nodes).
 * │  ├── binary_meta                                                           ← binaryMetaRoot (shared between all nodes).
 * │  ├── marshaller                                                            ← marshaller (shared between all nodes).
 * │  ├── snapshots                                                             ← snapshots (shared between all nodes).
 *
 * @see NodeFileTree
 */
public class SharedFileTree {
    /** Default path (relative to working directory) of binary metadata folder */
    public static final String DFLT_BINARY_METADATA_PATH = "binary_meta";

    /** Default path (relative to working directory) of marshaller mappings folder */
    public static final String DFLT_MARSHALLER_PATH = "marshaller";

    /** Root(work) directory. */
    protected final File root;

    /** db directory. */
    protected final File db;

    /** Path to the directory containing binary metadata. */
    protected final File binaryMetaRoot;

    /** Path to the directory containing marshaller files. */
    protected final File marshaller;

    /**
     * @param root Root directory.
     */
    public SharedFileTree(File root) {
        A.notNull(root, "Root directory");

        this.root = root;
        db = new File(root, DB_DEFAULT_FOLDER);
        marshaller = new File(db, DFLT_MARSHALLER_PATH);
        binaryMetaRoot = new File(db, DFLT_BINARY_METADATA_PATH);
    }

    /**
     * @param root Root directory.
     */
    public SharedFileTree(String root) {
        this(new File(root));
    }

    /**
     * @param cfg Config to get {@code root} directory from.
     */
    SharedFileTree(IgniteConfiguration cfg) {
        A.notNull(cfg, "config");

        try {
            root = new File(U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        db = new File(root, DB_DEFAULT_FOLDER);
        marshaller = new File(db, DFLT_MARSHALLER_PATH);
        binaryMetaRoot = new File(db, DFLT_BINARY_METADATA_PATH);
    }

    /**
     * @return Path to the {@code root} directory.
     */
    public File root() {
        return root;
    }

    /**
     * @return Path to the {@code db} directory.
     */
    public File db() {
        return db;
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

    /**
     * Creates {@link #binaryMetaRoot()} directory.
     * @return Created directory.
     * @see SharedFileTree#binaryMetaRoot()
     */
    public File mkdirBinaryMetaRoot() throws IgniteCheckedException {
        return mkdir(binaryMetaRoot, "root binary metadata");
    }

    /**
     * Creates {@link #marshaller()} directory.
     * @return Created directory.
     * @see #marshaller()
     */
    public File mkdirMarshaller() throws IgniteCheckedException {
        return mkdir(marshaller, "marshaller mappings");
    }

    /**
     * @param f File to check.
     * @return {@code True} if argument can be binary meta root directory.
     */
    public static boolean isBinaryMetaRoot(File f) {
        return f.getAbsolutePath().endsWith(DFLT_BINARY_METADATA_PATH);
    }

    /**
     * @param f File to check.
     * @return {@code True} if f ends with binary meta root directory.
     */
    public static boolean isMarshaller(File f) {
        return f.getAbsolutePath().endsWith(DFLT_MARSHALLER_PATH);
    }

    /**
     * @param file File to check.
     * @return {@code True} if {@code f} contains binary meta root directory.
     */
    public static boolean containsBinaryMetaPath(File file) {
        return file.getPath().contains(DFLT_BINARY_METADATA_PATH);
    }

    /**
     * @param f File to check.
     * @return {@code True} if {@code f} contains marshaller directory.
     */
    public static boolean containsMarshaller(File f) {
        return f.getAbsolutePath().contains(DFLT_MARSHALLER_PATH);
    }

    /**
     * @param dir Directory to create
     * @throws IgniteCheckedException
     */
    public static File mkdir(File dir, String name) throws IgniteCheckedException {
        if (!U.mkdirs(dir))
            throw new IgniteException("Could not create directory for " + name + ": " + dir);

        if (!dir.canRead())
            throw new IgniteCheckedException("Cannot read from directory: " + dir);

        if (!dir.canWrite())
            throw new IgniteCheckedException("Cannot write to directory: " + dir);

        return dir;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SharedFileTree.class, this);
    }
}
