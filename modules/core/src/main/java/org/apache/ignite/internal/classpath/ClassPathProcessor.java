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

package org.apache.ignite.internal.classpath;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.classpath.IgniteClassPathState.NEW;

/**
 * TODO:
 * 1. How to check data integrity on start?
 * Do we want to do this for txt file or for jar only?
 * 2. Check and remove obsolete icp from dist on start.
 * Do we want to have some flag to skip remove in this case? (if we preparing for ICP registration).
 * 3. Should we include CP into snapshots and dumps?
 * 4. Should we control file size to ensure disk free space enough to upload CP files?
 */
public class ClassPathProcessor extends GridProcessorAdapter {
    /** Prefix for metastorage keys. */
    private static final String METASTORE_PREFIX = "icp.";

    /** Handles download requests for {@link IgniteClassPath} files. */
    private final ClassPathFilesTransmissionHandler icpFilesHnd;

    /** Distributed process that deploys classpath files to all nodes. */
    private final DeployToAllProcess deployToAllProc;

    /**
     * @param ctx Kernal context.
     */
    public ClassPathProcessor(GridKernalContext ctx) {
        super(ctx);

        icpFilesHnd = new ClassPathFilesTransmissionHandler(ctx);
        deployToAllProc = new DeployToAllProcess(ctx, icpFilesHnd);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        icpFilesHnd.start();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        icpFilesHnd.stop();
    }

    /**
     * Register new classpath in metastorage it same name not exists.
     * Fails if exists.
     *
     * @param name Class path name.
     * @param files Files included.
     * @param lengths Files lengths.
     * @return Class path id.
     */
    public UUID startCreation(String name, String[] files, long[] lengths) {
        A.ensure(files.length == lengths.length, "wrong arrays lengths");
        A.ensure(U.alphanumericUnderscore(name), "Classpath name must satisfy the following name pattern: a-zA-Z0-9_");

        for (String file : files)
            ensureFilename(file);

        IgniteClassPath icp = new IgniteClassPath(UUID.randomUUID(), name, files, lengths, NEW);

        File root = ctx.pdsFolderResolver().fileTree().classPathRoot(name);

        Boolean metastorageWritten;

        try {
            createRootAndCheckIsEmpty(root);

            metastorageWritten = casToMetastorageAsync(null, icp).get();
        }
        catch (Exception e) {
            cleanup(icp, false);

            throw new IgniteException(e);
        }

        if (metastorageWritten != null && !metastorageWritten)
            throw new IgniteException("Fail to register ClassPath. Same ClassPath exists, already?");

        log.info("New IgniteClasspath created [root = " + root + ", icp=" + icp + ']');

        return icp.id();
    }

    /**
     * Writes {@code batch} to the Ignite Class Path file.
     *
     * @param icpId ClassPath id.
     * @param name File name.
     * @param offset Offset to write data to.
     * @param batch Batch.
     */
    public synchronized void writeFilePartFromClient(
        UUID icpId,
        String name,
        long offset,
        byte[] batch
    ) {
        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        try {
            ensureKnownFilename(name, icp);

            File root = ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name());

            File f = new File(root, name);

            if (offset == 0) {
                A.ensure(root.equals(f.getParentFile()), "filename");

                log.info("Creating new classpath file: " + f);

                if (!f.createNewFile())
                    throw new IgniteException("File exists: " + f);
            }

            try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                if (raf.length() < offset) {
                    throw new IgniteException("Wrong offset [icp=" + icp.name() + ", lib=" + name + ", " +
                        "fileLength=" + raf.length() + ", offset=" + offset + ']');
                }

                raf.seek(offset);
                raf.write(batch);
            }
        }
        catch (Throwable e) {
            log.error("Failed to upload ClassPath file, the ClassPath will be removed " +
                "[name=" + icp.name() + ", id=" + icpId + ", file=" + name + ']', e);

            cleanup(icp, false);

            throw new IgniteException(e);
        }
    }

    /**
     * Copies local file to class path directory.
     *
     * @param icpId ClassPath id.
     * @param file File to copy.
     */
    public void copyClassPathFileLocally(UUID icpId, Path file) {
        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        try {
            String name = file.getFileName().toString();

            ensureKnownFilename(name, icp);

            File root = ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name());

            Path f = new File(root, name).toPath();

            if (Files.exists(f)) {
                if(Files.isSameFile(file, f)) {
                    log.info("Skip copying new classpath file, already there: " + f);

                    return;
                }

                throw new IgniteException("File exists: " + f);
            }

            A.ensure(root.equals(f.toFile().getParentFile()), "filename");

            log.info("Copying new classpath file: " + f);

            Files.copy(file, f);
        }
        catch (Throwable e) {
            log.error("Failed to copy ClassPath file locally, the ClassPath will be removed " +
                "[name=" + icp.name() + ", id=" + icpId + ']', e);

            cleanup(icp, false);

            throw new IgniteException(e);
        }
    }

    /**
     * Deploy {@link IgniteClassPath} to all nodes.
     * @param icpId ClassPath id.
     * @return Future for process result.
     */
    public IgniteInternalFuture<?> deployToAll(@Nullable UUID icpId) {
        return deployToAllProc.start(icpId);
    }

    /**
     *
     */
    GridFutureAdapter<Boolean> casToMetastorageAsync(@Nullable IgniteClassPath prev, IgniteClassPath icp) {
        try {
            String key = metastorageKey(icp.name());

            if (log.isDebugEnabled())
                log.debug("Writing new ClassPath state [new=" + icp + ", prev=" + prev + ']');

            GridFutureAdapter<Boolean> res = ctx.distributedMetastorage().compareAndSetAsync(key, prev, icp);

            res.listen(casFut -> {
                if (casFut.error() == null)
                    return;

                try {
                    Object val = ctx.distributedMetastorage().read(key);

                    log.warning("Fail to write new ClassPath state [exp=" + prev + ", actual=" + val + ']');
                }
                catch (IgniteCheckedException e) {
                    log.warning("Can't read metastore key", e);
                }
            });

            return res;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param icpId ClassPath id.
     * @param ctx Kernal context.
     * @return Class path.
     */
    static IgniteClassPath fromMetastorage(UUID icpId, @Nullable IgniteClassPathState expState, GridKernalContext ctx) {
        try {
            IgniteClassPath[] icp = new IgniteClassPath[1];

            ctx.distributedMetastorage().iterate(METASTORE_PREFIX, (key, icp0) -> {
                if (icpId.equals(((IgniteClassPath)icp0).id()))
                    icp[0] = (IgniteClassPath)icp0;
            });

            if (icp[0] == null)
                throw new IgniteException("ClassPath not found: " + icpId);

            if (expState != null && icp[0].state() != expState)
                throw new IgniteException("ClassPath in wrong state [expected=" + expState + ", status=" + icp[0].state() + ']');

            return icp[0];
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public static String metastorageKey(String name) {
        return METASTORE_PREFIX + name;
    }

    /** */
    private static void ensureFilename(String file) {
        Path path = Path.of(file);

        A.ensure(path.getNameCount() == 1 && !path.isAbsolute(), "simple filename expected");
    }

    /** */
    private static void ensureKnownFilename(String name, IgniteClassPath icp) {
        ensureFilename(name);

        if (F.indexOf(icp.files(), name) == -1)
            throw new IllegalArgumentException("Unknown lib [icp=" + icp.name() + ", unknown_lib=" + name + ']');
    }

    /** */
    static void createRootAndCheckIsEmpty(File root) {
        if (!root.exists())
            NodeFileTree.mkdir(root, "Ignite Class Path root");
        else if (!F.isEmpty(root.listFiles()))
            throw new IgniteException("ClassPath root exists and not empty: " + root);
    }

    /**
     * Asynchronously removes the classpath metastorage record and deletes its local files.
     *
     * @param icp ClassPath to clean up.
     * @param loc If {@code true} then delete local files, only. Don't remove distributed key.
     */
    IgniteInternalFuture<Void> cleanupAsync(IgniteClassPath icp, boolean loc) {
        try {
            GridFutureAdapter<Void> res = new GridFutureAdapter<>();

            ctx.pools().getSystemExecutorService().submit(() -> {
                try {
                    cleanup(icp, loc);

                    res.onDone((Void)null);
                }
                catch (Throwable e) {
                    res.onDone(e);
                }
            });

            if (log.isDebugEnabled()) {
                res.listen(f -> {
                    if (log.isDebugEnabled())
                        log.debug("Async cleanup done. " + (res.error() == null ? "OK" : "FAIL"));
                });
            }

            return res;
        }
        catch (RejectedExecutionException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * Removes the classpath metastorage record and deletes its local files.
     *
     * @param icp ClassPath to clean up.
     * @param loc If {@code true} then delete local files, only. Don't remove distributed key.
     */
    void cleanup(IgniteClassPath icp, boolean loc) {
        if (!loc) {
            try {
                ctx.distributedMetastorage().remove(metastorageKey(icp.name()));
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to remove ClassPath metastorage record [name=" + icp.name() + ']', e);
            }
        }

        U.delete(ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name()));
    }
}
