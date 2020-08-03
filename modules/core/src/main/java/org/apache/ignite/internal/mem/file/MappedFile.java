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

package org.apache.ignite.internal.mem.file;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.UnsafeChunk;
import org.apache.ignite.internal.util.typedef.internal.U;
import sun.nio.ch.FileChannelImpl;

public class MappedFile implements Closeable, DirectMemoryRegion {
    /** */
    private static final Method map0 = U.findNonPublicMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);

    /** */
    private static final Method unmap0 = U.findNonPublicMethod(FileChannelImpl.class, "unmap0", long.class, long.class);

    /** */
    public static final int MAP_RW = 1;

    /** */
    private final RandomAccessFile file;

    /** */
    private final long addr;

    /** */
    private final long size;

    /**
     * @param name File path.
     * @param size Expected file size. If size is 0, the existing size will be used.
     * @throws IOException If failed to open the file or memory-map it.
     */
    public MappedFile(File name, long size) throws IOException {
        file = new RandomAccessFile(name, "rw");

        try {
            if (size == 0)
                size = file.length();
            else
                file.setLength(size);

            addr = map(file, MAP_RW, 0, size);

            this.size = size;
        } catch (IOException e) {
            file.close();

            throw e;
        }
    }

    /**
     * Gets underlying random access file.
     *
     * @return Random access file.
     */
    public final RandomAccessFile file() {
        return file;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        try {
            unmap(addr, size);
        }
        finally {
            file.close();
        }
    }

    /** {@inheritDoc} */
    @Override public final long address() {
        return addr;
    }

    /** {@inheritDoc} */
    @Override public final long size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public DirectMemoryRegion slice(long offset) {
        if (offset < 0 || offset >= size)
            throw new IllegalArgumentException("Failed to create a memory region slice [ptr=" + U.hexLong(addr) +
                ", len=" + size + ", offset=" + offset + ']');

        return new UnsafeChunk(addr + offset, size - offset);
    }

    /**
     * Maps the given region of the file.
     *
     * @param f File to map.
     * @param mode Mode to map.
     * @param start Mapping start offset within the file.
     * @param size Size of file to map.
     * @return Pointer to the mapped memory region.
     * @throws IOException
     */
    public static long map(RandomAccessFile f, int mode, long start, long size) throws IOException {
        try {
            return (Long) map0.invoke(f.getChannel(), mode, start, size);
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();
            throw (target instanceof IOException) ? (IOException) target : new IOException(target);
        }
    }

    /**
     * Un-maps the given region of the file.
     *
     * @param addr Previously mapped address to un-map.
     * @param size Size of the mapped file.
     */
    public static void unmap(long addr, long size) {
        try {
            unmap0.invoke(null, addr, size);
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        catch (InvocationTargetException e) {
            throw new IllegalStateException(e.getTargetException());
        }
    }
}
