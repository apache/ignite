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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.classpath.IgniteClassPathState.CREATING;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CLASSPATH_DEPLOY_TO_ALL;

/**
 * TODO:
 * 1. How to check data integrity on start?
 * Do we want to do this for txt file or for jar only?
 * 2. Check and remove obsolete icp from dist on start.
 * Do we want to have some flag to skip remove in this case? (if we preparing for ICP registration).
 * 3. Should we include CP into snapshots and dumps?
 */
public class ClassPathProcessor extends GridProcessorAdapter {
    /** Prefix for metastorage keys. */
    public static final String METASTORE_PREFIX = "classpath.";

    /** Distribute process that distributes new Ignite class path across all server nodes. */
    private final DistributedProcess<ClassPathDeployToAllRequest, ClassPathDeployToAllResponse> deployToAllProc;

    /** */
    private final Map<UUID, GridFutureAdapter<String>> deployToAllFuts = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public ClassPathProcessor(GridKernalContext ctx) {
        super(ctx);

        deployToAllProc = new DistributedProcess<>(
            ctx,
            CLASSPATH_DEPLOY_TO_ALL,
            this::startDeployToAllProcess,
            this::processDeployToAllResult
        );
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
        assert files.length == lengths.length : "wrong arrays lengths";

        A.ensure(U.alphanumericUnderscore(name), "Classpath name must satisfy the following name pattern: a-zA-Z0-9_");

        IgniteClassPath icp = new IgniteClassPath(UUID.randomUUID(), name, files, lengths);

        toMetastorage(icp, null);

        try {
            NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

            File root = ft.classPath(name);

            NodeFileTree.mkdir(root, "Ignite Class Path root: " + name);

            log.info("New classpath registered [root = " + root + ", icp=" + icp + ']');

            return icp.id();
        }
        catch (Exception e) {
            try {
                ctx.distributedMetastorage().remove(metastorageKey(icp));
            }
            catch (IgniteCheckedException ex) {
                log.error("Can't remove metastorage key for IgniteClassPath: " + metastorageKey(icp), e);
            }

            throw e;
        }
    }

    /**
     * @param icpID ClassPath ID.
     * @param name File name.
     * @param offset Offset to write data to.
     * @param bytesCnt Bytes count in batch to write.
     * @param batch Batch.
     */
    public void uploadFilePart(
        UUID icpID,
        String name,
        long offset,
        int bytesCnt,
        byte[] batch
    ) throws IOException {
        try {
            IgniteClassPath icp = fromMetastorage(icpID);

            if (F.indexOf(icp.files(), name) == -1)
                throw new IllegalArgumentException("Unknown lib [icp=" + icp.name() + ", unknown_lib=" + name + ']');

            File lib = new File(ctx.pdsFolderResolver().fileTree().classPath(icp.name()), name);

            if (offset == 0) {
                log.info("Creating new classpath file: " + lib);

                if (!lib.createNewFile())
                    throw new IgniteException("File exists: " + lib);
            }

            try (RandomAccessFile raf = new RandomAccessFile(lib, "rw")) {
                if (raf.length() < offset) {
                    throw new IgniteException("Wrong offset [icp=" + icp.name() + ", lib=" + name + ", " +
                        "fileLength=" + raf.length() + ", offset=" + offset + ']');
                }

                raf.seek(offset);
                raf.write(batch, 0, bytesCnt);
            }
        }
        catch (Exception e) {
            log.error("UploadFilePart:", e);

            throw e;
        }
    }

    /**
     * @param icpId ClassPath ID.
     * @return
     */
    public IgniteInternalFuture<?> distributeToAllNodes(UUID icpId) {
        GridFutureAdapter<String> fut = new GridFutureAdapter<>();

        synchronized (this) {
            IgniteClassPath icp = fromMetastorage(icpId);

            ClassPathDeployToAllRequest req = new ClassPathDeployToAllRequest(icpId);

            if (deployToAllFuts.put(icpId, fut) != null)
                return new GridFinishedFuture<>(new IllegalStateException("Distribute to all process started, already: " + icp));

            deployToAllProc.start(icpId, req);
        }

        return fut;
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<ClassPathDeployToAllResponse> startDeployToAllProcess(ClassPathDeployToAllRequest req) {
        IgniteClassPath icp = fromMetastorage(req.icpId);

        if (deployToAllFuts.containsKey(req.icpId)) {
            log.info("Upload node skip download [icp=" + icp + ']');

            return new GridFinishedFuture<>(new ClassPathDeployToAllResponse());
        }

        log.info("Starting download new classpath [icp=" + icp + ']');

        return new GridFinishedFuture<>(new ClassPathDeployToAllResponse());
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processDeployToAllResult(UUID id, Map<UUID, ClassPathDeployToAllResponse> res, Map<UUID, Throwable> err) {
        GridFutureAdapter<String> fut = deployToAllFuts.remove(id);

        // Only upload node manage the process.
        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Unknown distribute process [id=" + id + ']');

            return;
        }

        IgniteClassPath icp = fromMetastorage(id);

        // TODO: check this exception not failed all node.
        if (!fut.onDone("OK")) {
            throw new IllegalStateException("Distribute process in wrong state " +
                "[canceled=" + fut.isCancelled() + ", failed=" + fut.isFailed() + ", done=" + fut.isDone() + ']');
        }

        icp.state(IgniteClassPathState.READY);

        log.info("Deploy to all DONE!");
    }

    /**
     * @param icpID ClassPath ID.
     * @return Class path.
     */
    private IgniteClassPath fromMetastorage(UUID icpID) {
        try {
            IgniteClassPath[] icp = new IgniteClassPath[1];

            ctx.distributedMetastorage().iterate(METASTORE_PREFIX, (key, icp0) -> {
                if (icpID.equals(((IgniteClassPath)icp0).id()))
                    icp[0] = (IgniteClassPath)icp0;
            });

            if (icp[0] == null)
                throw new IgniteException("ClassPath not found: " + icpID);

            if (icp[0].state() != CREATING)
                throw new IgniteException("ClassPath in wrong state [expected=" + CREATING + ", status=" + icp[0].state() + ']');

            return icp[0];
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private void toMetastorage(IgniteClassPath icp, @Nullable IgniteClassPath prev) {
        try {
            if (!ctx.distributedMetastorage().compareAndSet(metastorageKey(icp), prev, icp))
                throw new IgniteException("Classpath alreay exists: " + icp.name());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private static String metastorageKey(IgniteClassPath icp) {
        return METASTORE_PREFIX + icp.name();
    }
}
