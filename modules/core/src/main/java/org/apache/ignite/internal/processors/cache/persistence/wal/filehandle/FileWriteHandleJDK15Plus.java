package org.apache.ignite.internal.processors.cache.persistence.wal.filehandle;

import static org.apache.ignite.internal.util.IgniteUtils.findField;
import static org.apache.ignite.internal.util.IgniteUtils.findNonPublicMethod;

import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

class FileWriteHandleJDK15Plus extends FileWriteHandleImpl {
    /**
     * @param cctx              Context.
     * @param fileIO            I/O file interface to use
     * @param rbuf
     * @param serializer        Serializer.
     * @param metrics           Data storage metrics.
     * @param writer            WAL writer.
     * @param pos               Initial position.
     * @param mode              WAL mode.
     * @param mmap              Mmap.
     * @param resume            Created on resume logging flag.
     * @param fsyncDelay        Fsync delay.
     * @param maxWalSegmentSize Max WAL segment size.
     * @throws IOException If failed.
     */
    FileWriteHandleJDK15Plus(GridCacheSharedContext cctx, SegmentIO fileIO, SegmentedRingByteBuffer rbuf, RecordSerializer serializer, DataStorageMetricsImpl metrics, FileHandleManagerImpl.WALWriter writer, long pos, WALMode mode, boolean mmap, boolean resume, long fsyncDelay, long maxWalSegmentSize) throws IOException {
        super(cctx, fileIO, rbuf, serializer, metrics, writer, pos, mode, mmap, resume, fsyncDelay, maxWalSegmentSize);
    }

    private static final Method force0 = findNonPublicMethod(
        U.classForName("java.nio.MappedMemoryUtils", null), "force0",
        java.io.FileDescriptor.class, long.class, long.class
    );

    private static final Field address = findField(MappedByteBuffer.class, "address");

    private static final Method mappingOffset = findNonPublicMethod(
        U.classForName("java.nio.MappedMemoryUtils", null), "mappingOffset", long.class
    );

    private static final Method mappingAddress = findNonPublicMethod(
        U.classForName("java.nio.MappedMemoryUtils", null), "mappingAddress", long.class, long.class
    );

    @Override
    protected void internalFsync(MappedByteBuffer buf, Object fieldObject, int off, int len) throws IllegalAccessException, InvocationTargetException {
        long bufAddr = address.getLong(buf);
        long mappedOff = (Long)mappingOffset.invoke(null, bufAddr);
        assert mappedOff == 0 : mappedOff;
        long addr = (Long)mappingAddress.invoke(null, bufAddr, mappedOff);

        long delta = (addr + off) % PAGE_SIZE;

        long alignedAddr = (addr + off) - delta;

        force0.invoke(buf, fieldObject, alignedAddr, len + delta);
    }
}
