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

package org.apache.ignite.igfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.hadoop.fs.HadoopFileSystemFactory;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.UniversalFileSystemAdapter;

/**
 * Universal adapter wrapping {@link org.apache.hadoop.fs.FileSystem} instance.
 */
public class HadoopFileSystemUniversalFileSystemAdapter implements UniversalFileSystemAdapter {
    /** File system factory. */
    private final HadoopFileSystemFactory factory;

    /**
     * Constructor.
     * @param factory File system factory.
     */
    public HadoopFileSystemUniversalFileSystemAdapter(HadoopFileSystemFactory factory) {
        assert factory != null;

        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public String name() throws IOException {
        return get().getUri().toString();
    }

    /** {@inheritDoc} */
    @Override public boolean exists(String path) throws IOException {
        return get().exists(new Path(path));
    }

    /** {@inheritDoc} */
    @Override public boolean delete(String path, boolean recursive) throws IOException {
        return get().delete(new Path(path), recursive);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(String path) throws IOException {
        boolean ok = get().mkdirs(new Path(path));
        if (!ok)
            throw new IOException("Failed to mkdirs: " + path);
    }

    /** {@inheritDoc} */
    @Override public void format() throws IOException {
        HadoopIgfsUtils.clear(get());
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties(String path) throws IOException {
        Path p = new Path(path);

        FileStatus status = get().getFileStatus(p);

        Map<String,String> m = new HashMap<>(3); // max size == 4

        m.put(IgfsEx.PROP_USER_NAME, status.getOwner());
        m.put(IgfsEx.PROP_GROUP_NAME, status.getGroup());

        FsPermission perm = status.getPermission();

        m.put(IgfsEx.PROP_PERMISSION, "0" + perm.getUserAction().ordinal() + perm.getGroupAction().ordinal() +
            perm.getOtherAction().ordinal());

        return m;
    }

    /** {@inheritDoc} */
    @Override public InputStream openInputStream(String path) throws IOException {
        return get().open(new Path(path));
    }

    /** {@inheritDoc} */
    @Override public OutputStream openOutputStream(String path, boolean append) throws IOException {
        Path p = new Path(path);

        if (append)
            return get().append(p);
        else
            return get().create(p, true/*overwrite*/);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if (HadoopFileSystemFactory.class.isAssignableFrom(cls))
            return (T)factory;

        return null;
    }

    /**
     * Create file system.
     *
     * @return File system.
     * @throws IOException If failed.
     */
    private FileSystem get() throws IOException {
        return factory.get(FileSystemConfiguration.DFLT_USER_NAME);
    }
}