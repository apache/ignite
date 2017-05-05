package org.apache.ignite.cache.database.pagemem;

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.database.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.database.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.database.MetadataStorageSelfTest;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.CIX3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.GridTestKernalContext;

/**
 *
 */
public class MetadataStoragePageMemoryImplSelfTest extends MetadataStorageSelfTest{
    /** Make sure page is small enough to trigger multiple pages in a linked list. */
    public static final int PAGE_SIZE = 1024;

    /** */
    private static File allocationPath;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        allocationPath = U.resolveWorkDirectory(U.defaultWorkDirectory(), "pagemem", false);
    }

    /**
     * @param clean Clean flag. If {@code true}, will clean previous memory state and allocate
     *      new empty page memory.
     * @return Page memory instance.
     */
    @Override protected PageMemory memory(boolean clean) throws Exception {
        long[] sizes = new long[10];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * 1024;

        DirectMemoryProvider provider = new MappedFileMemoryProvider(log(), allocationPath, clean, sizes);

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            new GridTestKernalContext(log),
            null,
            null,
            null,
            new NoOpPageStoreManager(),
            new NoOpWALManager(),
            new IgniteCacheDatabaseSharedManager(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        return new PageMemoryImpl(provider, sharedCtx, PAGE_SIZE,
            new CIX3<FullPageId, ByteBuffer, Integer>() {
            @Override public void applyx(FullPageId fullPageId, ByteBuffer byteBuf, Integer tag) {
                assert false : "No evictions should happen during the test";
            }
        }, new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
            @Override public void applyx(Long page, FullPageId fullId, PageMemoryEx pageMem) {
            }
        }, new CheckpointLockStateChecker() {
            @Override public boolean checkpointLockIsHeldByThread() {
                return true;
            }
        });
    }
}
