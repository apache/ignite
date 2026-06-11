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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.NEW;

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
    public static final String METASTORE_PREFIX = "icp.";

    /** Handles download requests for {@link IgniteClassPath} files. */
    private final ClassPathFilesTransmissionHandler icpFilesHnd;

    /** Distributed process that deploys classpath files to all nodes. */
    private final DeployToAllProcess deployToAllProc;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

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
        ctx.event().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            // TODO: check busyLock required. See IgniteSnapshotManager.
            UUID leftNodeId = evt.eventNode().id();

            if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
                icpFilesHnd.onNodeLeft(leftNodeId);
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        icpFilesHnd.stop();

        if (discoLsnr != null)
            ctx.event().removeDiscoveryEventListener(discoLsnr);
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

        // TODO: allow dot here.
        A.ensure(U.alphanumericUnderscore(name), "Classpath name must satisfy the following name pattern: a-zA-Z0-9_");

        IgniteClassPath icp = new IgniteClassPath(UUID.randomUUID(), name, files, lengths, NEW);

        NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

        File root = ft.classPathRoot(name);

        try {
            casToMetastorage(null, icp);

            NodeFileTree.mkdir(root, "Ignite Class Path root: " + name);
        }
        catch (Exception e) {
            try {
                ctx.distributedMetastorage().remove(metastorageKey(icp));

                U.delete(root);
            }
            catch (IgniteCheckedException ex) {
                log.error("Cleanup after IgniteClassPath creation failed [key=" + metastorageKey(icp) + ", root=" + root + ']', e);
            }

            throw e;
        }

        log.info("New classpath created [root = " + root + ", icp=" + icp + ']');

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
    public void writeFilePartFromClient(
        UUID icpId,
        String name,
        long offset,
        byte[] batch
    ) throws IOException {
        try {
            IgniteClassPath icp = fromMetastorage(icpId, ctx);

            if (F.indexOf(icp.files(), name) == -1)
                throw new IllegalArgumentException("Unknown lib [icp=" + icp.name() + ", unknown_lib=" + name + ']');

            File f = new File(ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name()), name);

            if (offset == 0) {
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
        catch (Exception e) {
            log.error("Upload file part error :", e);

            throw e;
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

    /** */
    private void casToMetastorage(@Nullable IgniteClassPath prev, IgniteClassPath icp) {
        try {
            if (!ctx.distributedMetastorage().compareAndSet(metastorageKey(icp), prev, icp))
                throw new IgniteException("Classpath alreay exists: " + icp.name());
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
    static IgniteClassPath fromMetastorage(UUID icpId, GridKernalContext ctx) {
        try {
            IgniteClassPath[] icp = new IgniteClassPath[1];

            ctx.distributedMetastorage().iterate(METASTORE_PREFIX, (key, icp0) -> {
                if (icpId.equals(((IgniteClassPath)icp0).id()))
                    icp[0] = (IgniteClassPath)icp0;
            });

            if (icp[0] == null)
                throw new IgniteException("ClassPath not found: " + icpId);

            if (icp[0].state() != NEW)
                throw new IgniteException("ClassPath in wrong state [expected=" + NEW + ", status=" + icp[0].state() + ']');

            return icp[0];
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
