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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * A memory-resident {@link Directory} implementation.
 */
public class GridLuceneDirectory extends Directory {
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
    public GridLuceneDirectory(GridUnsafeMemory mem) {
        this.mem = mem;

        try {
            setLockFactory(new GridLuceneLockFactory());
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public final String[] listAll() {
        ensureOpen();
        // NOTE: fileMap.keySet().toArray(new String[0]) is broken in non Sun JDKs,
        // and the code below is resilient to map changes during the array population.
        Set<String> fileNames = fileMap.keySet();

        List<String> names = new ArrayList<>(fileNames.size());

        for (String name : fileNames)
            names.add(name);

        return names.toArray(new String[names.size()]);
    }

    /** {@inheritDoc} */
    @Override public final boolean fileExists(String name) {
        ensureOpen();

        return fileMap.containsKey(name);
    }

    /** {@inheritDoc} */
    @Override public final long fileModified(String name) {
        ensureOpen();

        throw new IllegalStateException(name);
    }

    /**
     * Set the modified time of an existing file to now.
     *
     * @throws IOException if the file does not exist
     */
    @Override public void touchFile(String name) throws IOException {
        ensureOpen();

        throw new IllegalStateException(name);
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

        doDeleteFile(name);
    }

    /**
     * Deletes file.
     *
     * @param name File name.
     * @throws IOException If failed.
     */
    private void doDeleteFile(String name) throws IOException {
        GridLuceneFile file = fileMap.remove(name);

        if (file != null) {
            file.delete();

            sizeInBytes.addAndGet(-file.getSizeInBytes());
        }
        else
            throw new FileNotFoundException(name);
    }

    /** {@inheritDoc} */
    @Override public IndexOutput createOutput(String name) throws IOException {
        ensureOpen();

        GridLuceneFile file = newRAMFile();

        GridLuceneFile existing = fileMap.remove(name);

        if (existing != null) {
            sizeInBytes.addAndGet(-existing.getSizeInBytes());

            existing.delete();
        }

        fileMap.put(name, file);

        return new GridLuceneOutputStream(file);
    }

    /**
     * Returns a new {@link GridLuceneFile} for storing data. This method can be
     * overridden to return different {@link GridLuceneFile} impls, that e.g. override.
     *
     * @return New ram file.
     */
    protected GridLuceneFile newRAMFile() {
        return new GridLuceneFile(this);
    }

    /** {@inheritDoc} */
    @Override public IndexInput openInput(String name) throws IOException {
        ensureOpen();

        GridLuceneFile file = fileMap.get(name);

        if (file == null)
            throw new FileNotFoundException(name);

        return new GridLuceneInputStream(name, file);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        isOpen = false;

        for (String fileName : fileMap.keySet()) {
            try {
                doDeleteFile(fileName);
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        assert fileMap.isEmpty();
    }

    /**
     * @return Memory.
     */
    GridUnsafeMemory memory() {
        return mem;
    }
}