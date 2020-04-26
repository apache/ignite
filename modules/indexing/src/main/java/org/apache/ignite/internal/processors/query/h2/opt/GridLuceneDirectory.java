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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

/**
 * A memory-resident {@link Directory} implementation.
 */
public class GridLuceneDirectory extends BaseDirectory implements Accountable {
    /** */
    protected final Map<String, GridLuceneFile> fileMap = new ConcurrentHashMap<>();

    /** */
    protected final AtomicLong sizeInBytes = new AtomicLong();

    /** */
    private final GridUnsafeMemory mem;

    /**
     * Constructs an empty {@link Directory}.
     *
     * @param mem Memory.
     */
    GridLuceneDirectory(GridUnsafeMemory mem) {
        super(new GridLuceneLockFactory());

        this.mem = mem;
    }

    /** {@inheritDoc} */
    @Override public final String[] listAll() {
        ensureOpen();
        // NOTE: fileMap.keySet().toArray(new String[0]) is broken in non Sun JDKs,
        // and the code below is resilient to map changes during the array population.
        Set<String> fileNames = fileMap.keySet();

        List<String> names = new ArrayList<>(fileNames);

        return names.toArray(new String[names.size()]);
    }

    /** {@inheritDoc} */
    @Override public void rename(String source, String dest) throws IOException {
        ensureOpen();

        GridLuceneFile file = fileMap.get(source);

        if (file == null)
            throw new FileNotFoundException(source);

        fileMap.put(dest, file);

        fileMap.remove(source);
    }

    /** {@inheritDoc} */
    @Override public void syncMetaData() throws IOException {
        // Noop. No meta data sync needed as all data is in-memory.
    }

    /** {@inheritDoc} */
    @Override public IndexOutput createTempOutput(String prefix, String suffix, IOContext ctx) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public final long fileLength(String name) throws IOException {
        ensureOpen();

        GridLuceneFile file = fileMap.get(name);

        if (file == null)
            throw new FileNotFoundException(name);

        return file.getLength();
    }

    /** {@inheritDoc} */
    @Override public void deleteFile(String name) throws IOException {
        ensureOpen();

        doDeleteFile(name, false);
    }

    /**
     * Deletes file.
     *
     * @param name File name.
     * @param onClose If on close directory;
     * @throws IOException If failed.
     */
    private void doDeleteFile(String name, boolean onClose) throws IOException {
        GridLuceneFile file = fileMap.remove(name);

        if (file != null) {
            file.delete();

            // All files should be closed when Directory is closing.
            assert !onClose || !file.hasRefs() : "Possible memory leak, resource is not closed: " + file.toString();

            sizeInBytes.addAndGet(-file.getSizeInBytes());
        }
        else
            throw new FileNotFoundException(name);
    }

    /** {@inheritDoc} */
    @Override public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        ensureOpen();

        GridLuceneFile file = new GridLuceneFile(this);

        // Lock for using in stream. Will be unlocked on stream closing.
        file.lockRef();

        GridLuceneFile existing = fileMap.put(name, file);

        if (existing != null) {
            sizeInBytes.addAndGet(-existing.getSizeInBytes());

            existing.delete();
        }

        return new GridLuceneOutputStream(file);
    }

    /** {@inheritDoc} */
    @Override public void sync(final Collection<String> names) throws IOException {
        // Noop. No fsync needed as all data is in-memory.
    }

    /** {@inheritDoc} */
    @Override public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();

        GridLuceneFile file = fileMap.get(name);

        if (file == null)
            throw new FileNotFoundException(name);

        // Lock for using in stream. Will be unlocked on stream closing.
        file.lockRef();

        if (!fileMap.containsKey(name)) {
            // Unblock for deferred delete.
            file.releaseRef();

            throw new FileNotFoundException(name);
        }

        return new GridLuceneInputStream(name, file);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        isOpen = false;

        IgniteException errs = null;

        for (String fileName : fileMap.keySet()) {
            try {
                doDeleteFile(fileName, true);
            }
            catch (IOException e) {
                if (errs == null)
                    errs = new IgniteException("Failed to close index directory." +
                    " Some index readers weren't closed properly, that may leads memory leak.");

                errs.addSuppressed(e);
            }
        }

        assert fileMap.isEmpty();

        if (errs != null && !F.isEmpty(errs.getSuppressed()))
            throw errs;
    }

    /** {@inheritDoc} */
    @Override public long ramBytesUsed() {
        ensureOpen();

        return sizeInBytes.get();
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<Accountable> getChildResources() {
        return Accountables.namedAccountables("file", new HashMap<>(fileMap));
    }

    /**
     * @return Memory.
     */
    GridUnsafeMemory memory() {
        return mem;
    }
}
