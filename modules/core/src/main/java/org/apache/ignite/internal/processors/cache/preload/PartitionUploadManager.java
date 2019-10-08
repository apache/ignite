package org.apache.ignite.internal.processors.cache.preload;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridCachePreloadSharedManager.rebalanceThreadTopic;

/**
 *
 */
public class PartitionUploadManager {
    /** */
    private GridCacheSharedContext<?, ?> cctx;

    /** */
    private IgniteLogger log;

    /** */
    private final ConcurrentMap<UUID, CachePartitionUploadFuture> uploadFutMap = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
//    private IgniteBackupPageStoreManager backupMgr;

    /**
     * @param ktx Kernal context to process.
     */
    public PartitionUploadManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config());

        cctx = ktx.cache().context();

        log = ktx.log(getClass());
    }

//    /**
//     * @return {@code True} if cluster rebalance via sending partition files can be applied.
//     */
//    public static boolean persistenceRebalanceApplicable(GridCacheSharedContext cctx) {
//        return !cctx.kernalContext().clientNode() &&
//            CU.isPersistenceEnabled(cctx.kernalContext().config()) &&
//            cctx.isRebalanceEnabled();
//    }

    /**
     * @param cctx Cache shared context.
     */
    public void start0(GridCacheSharedContext<?, ?> cctx) throws IgniteCheckedException {
        this.cctx = cctx;

        //backupMgr = cctx.storeBackup();

//        if (persistenceRebalanceApplicable(cctx)) {
//            cctx.gridIO().addMessageListener(rebalanceThreadTopic(), new GridMessageListener() {
//                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
//                    if (msg instanceof GridPartitionBatchDemandMessage) {
//                        // Start to checkpoint and upload process.
//                        lock.readLock().lock();
//
//                        try {
//                            onDemandMessage0(nodeId, (GridPartitionBatchDemandMessage)msg, plc);
//                        }
//                        finally {
//                            lock.readLock().unlock();
//                        }
//                    }
//                }
//            });
//        }
    }

    /**
     * @param cancel <tt>true</tt> to cancel all pending tasks.
     */
    public void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            cctx.gridIO().removeMessageListener(rebalanceThreadTopic());

            for (CachePartitionUploadFuture fut : uploadFutMap.values())
                fut.cancel();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * This internal method will handle demand requests of copying cache group partitions to the remote node.
     * It will perform checkpoint operation to take the latest partitions changes for list of demaned cache
     * groups and upload them one by one.
     *
     * @param nodeId The nodeId request comes from.
     * @param msg Message containing rebalance request params.
     */
    public void onDemandMessage(UUID nodeId, GridPartitionBatchDemandMessage msg, byte plc) {
// todo
//        IgniteSocketChannel ch = null;
//        CachePartitionUploadFuture uploadFut = null;
//
        CachePartitionUploadFuture uploadFut = null;

        log.info("Processing demand message from " + nodeId);

        try {
            // todo compute if absent?
            synchronized (uploadFutMap) {
                // todo why we need this global mapping
                uploadFut = uploadFutMap.getOrDefault(nodeId,
                    new CachePartitionUploadFuture(msg.rebalanceId(), msg.topologyVersion(), msg.assignments()));

                if (uploadFut.rebalanceId < msg.rebalanceId()) {
                    if (!uploadFut.isDone()) {
                        log.info("Restarting upload routine [node=" + nodeId + ", old=" + uploadFut.rebalanceId + ", new=" + msg.rebalanceId());

                        uploadFut.cancel();
                    }

                    uploadFutMap.put(nodeId,
                        uploadFut = new CachePartitionUploadFuture(msg.rebalanceId(),
                            msg.topologyVersion(),
                            msg.assignments()));
                }
            }

            // Need to start new partition upload routine.
//            ch = cctx.gridIO().channelToTopic(nodeId, rebalanceThreadTopic(), plc);

            // History should be reserved on exchange done.

//            for (Map.Entry<Integer, Set<Integer>> e : uploadFut.getAssigns().entrySet()) {
//                int grpId = e.getKey();
//
//                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);
//
//                // todo handle exceptions somehow
//                // todo should we reserve partition when sending
////                for (int partId : e.getValue()) {
////                    GridDhtLocalPartition part = grp.topology().localPartition(partId);
////
////                    boolean reserved = part.reserve();
////
////                    assert reserved : part.id();
////
//////                    long updateCntr = part.updateCounter();
////
//////                    boolean histReserved = cctx.database().reserveHistoryForPreloading(grpId, partId, updateCntr);
//////
//////                    assert histReserved : part.id();
//////
//////                    if (log.isDebugEnabled())
//////                        log.debug("Reserved history for preloading [grp=" + grp.cacheOrGroupName() + ", part=" + partId + ", cntr=" + updateCntr);
////                }
//            }

            // todo - exec trnasmission on supplier thread!
            // History should be reserved on exchange done.
            sendPartitions(uploadFut, nodeId).get();

//            backupMgr.backup(uploadFut.rebalanceId,
//                uploadFut.getAssigns(),
//                new SocketBackupProcessSupplier(
//                    new FileTransferManager<>(cctx.kernalContext(), ch.channel(), uploadFut),
//                    log
//                ),
//                uploadFut);
        }
        catch (Exception e) {
            U.error(log, "An error occured while processing initial demand request ["
                + ", nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']', e);

            if (uploadFut != null)
                uploadFut.onDone(e);
        }
//        finally {
//            U.closeQuiet(ch);
//        }
    }

    /**
     * @param fut Future.
     * @param nodeId Node id.
     */
    private IgniteInternalFuture sendPartitions(CachePartitionUploadFuture fut, UUID nodeId) throws IgniteCheckedException {
        File tmpDir = new File(IgniteSystemProperties.getString("java.io.tmpdir"));

        assert tmpDir.exists() : tmpDir;

        if (log.isDebugEnabled())
            log.debug("Creating partitions snapshot for node=" + nodeId + " in " + tmpDir);

        String backupDir = "preload-" + fut.rebalanceId;

        cctx.backup().createLocalBackup(backupDir, fut.getAssigns(), tmpDir).get();

//        cctx.preloader().offerCheckpointTask(() -> {
//            try {
//                Map<Integer, Map<Integer, File>> filesToSnd = new HashMap<>();
//
//                for (Map.Entry<Integer, Set<Integer>> e : fut.getAssigns().entrySet()) {
//
//                    int grpId = e.getKey();
//
//                    Map<Integer, File> partFiles = new HashMap<>();
//
//                    for (int partId : e.getValue()) {
//                        String path = cctx.preloader().storePath(grpId, partId);
//
//                        File src = new File(path);
//                        File dest = new File(path +  ".cpy");
//
//                        log.info("Copying file \"" + src + "\" to \"" + dest + "\"");
//
//                        RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();
//
//                        GridFileUtils.copy(ioFactory, src, ioFactory, dest, Long.MAX_VALUE);
//
//                        partFiles.put(partId, dest);
//                    }
//
//                    filesToSnd.put(grpId, partFiles);
//                }
//
//                fut.partFiles(filesToSnd);
//            } catch (IgniteCheckedException | IOException e) {
//                fut.onDone(e);
//            }
//        }).listen(
//            c -> {
        // send files
        GridIoManager io = cctx.kernalContext().io();

        String dir = tmpDir + "/" + backupDir + "/";

        try (GridIoManager.TransmissionSender snd = io.openTransmissionSender(nodeId, rebalanceThreadTopic())) {
            try {
                for (Map.Entry<Integer, Set<Integer>> e : fut.getAssigns().entrySet()) {
                    Integer grpId = e.getKey();

                    String grpDir = dir + FilePageStoreManager.cacheDirName(cctx.cache().cacheGroup(grpId).config());

                    for (Integer partId : e.getValue()) {
                        File file = new File(grpDir + "/" + "part-" + partId + ".bin");

                        assert file.exists() : file;

                        snd.send(file, F.asMap("group", grpId, "part", partId), TransmissionPolicy.FILE);

                        GridDhtLocalPartition part = cctx.cache().cacheGroup(grpId).topology().localPartition(partId);

                        // todo release only once - after historical rebalancing
                        part.release();
                    }
                }
            } finally {
                U.delete(new File(dir));
            }

            fut.onDone();
        }
        catch (IOException | IgniteCheckedException | InterruptedException e) {
            fut.onDone(e);
        }
        //todo should we cleanup files on error?
//            }
//        );

//        if (!fut.isDone())
//            cctx.database().wakeupForCheckpoint(String.format(REBALANCE_CP_REASON, fut.getAssigns().keySet()));

        // todo
        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionUploadManager.class, this);
    }

    /** */
//    private static class SocketBackupProcessSupplier implements BackupProcessSupplier {
//        /** */
//        private final FileTransferManager<PartitionFileMetaInfo> ftMrg;
//
//        /** */
//        private final IgniteLogger log;
//
//        /**
//         * @param ftMrg An upload helper class.
//         */
//        public SocketBackupProcessSupplier(FileTransferManager<PartitionFileMetaInfo> ftMrg, IgniteLogger log) {
//            this.ftMrg = ftMrg;
//            this.log = log;
//        }
//
//        /** {@inheritDoc} */
//        @Override public void supplyPartition(
//            GroupPartitionId grpPartId,
//            File file,
//            long size
//        ) throws IgniteCheckedException {
//            U.log(log, "Start partition meta info uploading: " + grpPartId);
//
//            ftMrg.writeMetaFrom(new PartitionFileMetaInfo(grpPartId.getGroupId(),
//                grpPartId.getPartitionId(),
//                file.getName(),
//                size,
//                0));
//
//            U.log(log, "Start partition uploading: " + file.getName());
//
//            ftMrg.writeFrom(file, 0, size);
//        }
//
//        /** {@inheritDoc} */
//        @Override public void supplyDelta(
//            GroupPartitionId grpPartId,
//            File file,
//            long offset,
//            long size
//        ) throws IgniteCheckedException {
//            U.log(log, "Start delta meta info uploading: " + grpPartId);
//
//            ftMrg.writeMetaFrom(new PartitionFileMetaInfo(grpPartId.getGroupId(),
//                grpPartId.getPartitionId(),
//                file.getName(),
//                size,
//                1));
//
//            U.log(log, "Start delta uploading: " + file.getName());
//
//            ftMrg.writeFrom(file, offset, size);
//        }
//    }

    /** */
    private static class CachePartitionUploadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private long rebalanceId;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, GridIntList> assigns;

        private Map<Integer, Map<Integer, File>> filesToSend;

        /** */
        public CachePartitionUploadFuture(
            long rebalanceId,
            AffinityTopologyVersion topVer,
            Map<Integer, GridIntList> assigns
        ) {
            this.rebalanceId = rebalanceId;
            this.topVer = topVer;
            this.assigns = assigns;
        }

        /**
         * @return The map of assignments of each cache group.
         */
        public Map<Integer, Set<Integer>> getAssigns() {
            Map<Integer, Set<Integer>> result = new HashMap<>();

            for (Map.Entry<Integer, GridIntList> grpPartsEntry : assigns.entrySet()) {
                GridIntIterator iterator = grpPartsEntry.getValue().iterator();

                result.putIfAbsent(grpPartsEntry.getKey(), new HashSet<>());

                while (iterator.hasNext())
                    result.get(grpPartsEntry.getKey()).add(iterator.next());
            }

            return result;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CachePartitionUploadFuture.class, this);
        }

        public void partFiles(Map<Integer, Map<Integer, File>> send) {
            filesToSend = send;
        }

        public Map<Integer, Map<Integer, File>> partFiles() {
            return filesToSend;
        }
    }
}
