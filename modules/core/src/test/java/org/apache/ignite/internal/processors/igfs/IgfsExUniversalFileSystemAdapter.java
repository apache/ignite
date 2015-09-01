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

package org.apache.ignite.internal.processors.igfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;

/**
 * Universal adapter over {@link IgfsEx} filesystem.
 */
public class IgfsExUniversalFileSystemAdapter implements UniversalFileSystemAdapter {

    /** The wrapped igfs. */
    private final IgfsEx igfsEx;

    /**
     * Constructor.
     * @param igfsEx the igfs to be wrapped.
     */
    public IgfsExUniversalFileSystemAdapter(IgfsEx igfsEx) {
        this.igfsEx = igfsEx;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return igfsEx.name();
    }

    /** {@inheritDoc} */
    @Override public boolean exists(String path) {
        return igfsEx.exists(new IgfsPath(path));
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(String path) throws IOException {
        igfsEx.mkdirs(new IgfsPath(path));
    }

    /** {@inheritDoc} */
    @Override public void format() throws IOException {
        igfsEx.format();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties(String path) {
        return igfsEx.info(new IgfsPath(path)).properties();
    }

    /** {@inheritDoc} */
    @Override public boolean delete(String path, boolean recursive) throws IOException {
        IgfsPath igfsPath = new IgfsPath(path);

        boolean del = igfsEx.delete(igfsPath, recursive);

        return del;
    }

    /** {@inheritDoc} */
    @Override public InputStream openInputStream(String path) throws IOException {
        IgfsPath igfsPath = new IgfsPath(path);

        IgfsInputStreamAdapter adapter = igfsEx.open(igfsPath);

        return adapter;
    }

    /** {@inheritDoc} */
    @Override public OutputStream openOutputStream(String path, boolean append) throws IOException {
        IgfsPath igfsPath = new IgfsPath(path);

        final IgfsOutputStream igfsOutputStream;
        if (append)
            igfsOutputStream = igfsEx.append(igfsPath, true/*create*/);
         else
            igfsOutputStream = igfsEx.create(igfsPath, true/*overwrite*/);

        return igfsOutputStream;
    }

    /** {@inheritDoc} */
    @Override public <T> T getAdapter(Class<T> clazz) {
        if (clazz == IgfsEx.class)
            return (T)igfsEx;

        return null;
    }
}