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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

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
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemFactoryDelegate;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.processors.igfs.IgfsSecondaryFileSystemTestAdapter;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Universal adapter wrapping {@link org.apache.hadoop.fs.FileSystem} instance.
 */
public class HadoopIgfsSecondaryFileSystemTestAdapter implements IgfsSecondaryFileSystemTestAdapter {
    /** File system factory. */
    private final HadoopFileSystemFactoryDelegate factory;

    /**
     * Constructor.
     * @param factory File system factory.
     */
    public HadoopIgfsSecondaryFileSystemTestAdapter(HadoopFileSystemFactory factory) {
        assert factory != null;

        this.factory = HadoopDelegateUtils.fileSystemFactoryDelegate(getClass().getClassLoader(), factory);

        this.factory.start();
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

        Map<String,String> m = new HashMap<>(3);

        m.put(IgfsUtils.PROP_USER_NAME, status.getOwner());
        m.put(IgfsUtils.PROP_GROUP_NAME, status.getGroup());
        m.put(IgfsUtils.PROP_PERMISSION, permission(status));

        return m;
    }

    /** {@inheritDoc} */
    @Override public String permissions(String path) throws IOException {
        return permission(get().getFileStatus(new Path(path)));
    }

    /**
     * Get permission for file status.
     *
     * @param status Status.
     * @return Permission.
     */
    private String permission(FileStatus status) {
        FsPermission perm = status.getPermission();

        return "0" + perm.getUserAction().ordinal() + perm.getGroupAction().ordinal() + perm.getOtherAction().ordinal();
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
    @Override public T2<Long, Long> times(String path) throws IOException {
        FileStatus status = get().getFileStatus(new Path(path));

        return new T2<>(status.getModificationTime(), status.getAccessTime());
    }

    /** {@inheritDoc} */
    @Override public IgfsEx igfs() {
        return null;
    }

    /**
     * Create file system.
     *
     * @return File system.
     * @throws IOException If failed.
     */
    protected FileSystem get() throws IOException {
        return (FileSystem)factory.get(FileSystemConfiguration.DFLT_USER_NAME);
    }
}
