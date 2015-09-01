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
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.UniversalFileSystemAdapter;

/**
 * Universal adapter wrapping {@link org.apache.hadoop.fs.FileSystem} instance.
 */
public class HadoopFileSystemUniversalFileSystemAdapter implements UniversalFileSystemAdapter {

    /** The wrapped filesystem. */
    private final FileSystem fileSys;

    /**
     * Constructor.
     * @param fs the filesystem to be wrapped.
     */
    public HadoopFileSystemUniversalFileSystemAdapter(FileSystem fs) {
        this.fileSys = fs;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return fileSys.getUri().toString();
    }

    /** {@inheritDoc} */
    @Override public boolean exists(String path) throws IOException {
        return fileSys.exists(new Path(path));
    }

    /** {@inheritDoc} */
    @Override public boolean delete(String path, boolean recursive) throws IOException {
        boolean ok = fileSys.delete(new Path(path), recursive);
        return ok;
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(String path) throws IOException {
        boolean ok = fileSys.mkdirs(new Path(path));
        if (!ok)
            throw new IOException("Failed to mkdirs: " + path);
    }

    /** {@inheritDoc} */
    @Override public void format() throws IOException {
        fileSys.delete(new Path("/"), true);
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties(String path) throws IOException {
        Path p = new Path(path);

        FileStatus status = fileSys.getFileStatus(p);

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
        return fileSys.open(new Path(path));
    }

    /** {@inheritDoc} */
    @Override public OutputStream openOutputStream(String path, boolean append) throws IOException {
        Path p = new Path(path);

        if (append)
            return fileSys.append(p);
        else
            return fileSys.create(p, true/*overwrite*/);
    }

    /** {@inheritDoc} */
    @Override public <T> T getAdapter(Class<T> clazz) {
        if (clazz == FileSystem.class)
            return (T)fileSys;

        return null;
    }
}