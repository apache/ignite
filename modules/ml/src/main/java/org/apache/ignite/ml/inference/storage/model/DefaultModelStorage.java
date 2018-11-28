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

package org.apache.ignite.ml.inference.storage.model;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Default implementation of {@link ModelStorage} that can use any {@link ModelStorageProvider} as a backend storage
 * system.
 */
public class DefaultModelStorage implements ModelStorage {
    /** Ignite Cache that is used to store model storage files. */
    private final ModelStorageProvider storageProvider;

    /**
     * Constructs a new instance of Ignite model storage.
     *
     * @param storageProvider Model storage provider.
     */
    public DefaultModelStorage(ModelStorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    /** {@inheritDoc} */
    @Override public void putFile(String path, byte[] data) {
        String parentPath = getParent(path);

        // Paths are locked in child-first order.
        Lock pathLock = storageProvider.lock(path);
        Lock parentPathLock = storageProvider.lock(parentPath);

        pathLock.lock();
        parentPathLock.lock();

        try {
            FileOrDirectory parent = storageProvider.get(parentPath);

            // If parent doesn't exist throw an exception.
            if (parent == null)
                throw new IllegalStateException("Cannot create file because directory doesn't exist [path="
                    + path + "]");

            // If parent is not a directory throw an exception.
            if (!parent.isDirectory())
                throw new IllegalStateException("Cannot create file because parent is not a directory [path="
                    + path + "]");

            Directory dir = (Directory) parent;
            // Update parent if it's a new file.
            if (!dir.getFiles().contains(path)) {
                dir.getFiles().add(path);
                storageProvider.put(parentPath, parent);
            }

            // Save file into cache.
            storageProvider.put(path, new File(data));
        }
        finally {
            parentPathLock.unlock();
            pathLock.unlock();
        }
    }

    /**  {@inheritDoc}*/
    @Override public byte[] getFile(String path) {
        FileOrDirectory fileOrDir = storageProvider.get(path);

        Lock pathLock = storageProvider.lock(path);

        pathLock.lock();

        try {
            // If file doesn't exist throw an exception.
            if (fileOrDir == null)
                throw new IllegalStateException("File doesn't exist [path=" + path + "]");

            // If file is not a regular file throw an exception.
            if (!fileOrDir.isFile())
                throw new IllegalStateException("File is not a regular file [path=" + path + "]");

            return ((File) fileOrDir).getData();
        }
        finally {
            pathLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdir(String path) {
        String parentPath = getParent(path);

        // Paths are locked in child-first order.
        Lock pathLock = storageProvider.lock(path);
        Lock parentPathLock = storageProvider.lock(parentPath);

        pathLock.lock();
        parentPathLock.lock();

        try {
            // If a directory associated with specified path exists return.
            if (isDirectory(path))
                return;

            // If a regular file associated with specified path exists throw an exception.
            if (isFile(path))
                throw new IllegalStateException("File with specified path already exists [path=" + path + "]");

            FileOrDirectory parent = storageProvider.get(parentPath);

            // If parent doesn't exist throw an exception.
            if (parent == null)
                throw new IllegalStateException("Cannot create directory because parent directory doesn't exist [path="
                    + path + "]");

            // If parent is not a directory throw an exception.
            if (!parent.isDirectory())
                throw new IllegalStateException("Cannot create directory because parent is not a directory [path="
                    + path + "]");

            Directory dir = (Directory) parent;
            dir.getFiles().add(path);

            // Update parent and save directory into cache.
            storageProvider.put(parentPath, parent);
            storageProvider.put(path, new Directory());
        }
        finally {
            parentPathLock.unlock();
            pathLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(String path) {
        Deque<IgniteBiTuple<String, Lock>> pathsToBeCreated = new LinkedList<>();

        IgniteBiTuple<String, Lock> parentWithLock = null;

        try {
            while (path != null) {
                // Paths are locked in child-first order.
                Lock lock = storageProvider.lock(path);
                lock.lock();

                pathsToBeCreated.push(new IgniteBiTuple<>(path, lock));

                if (exists(path)) {
                    if (isDirectory(path)) {
                        parentWithLock = pathsToBeCreated.pop();
                        break;
                    }

                    throw new IllegalStateException("Cannot create directory because parent is not a directory [path="
                        + path + "]");
                }

                path = getParent(path);
            }

            while (!pathsToBeCreated.isEmpty()) {
                IgniteBiTuple<String, Lock> pathWithLock = pathsToBeCreated.pop();

                storageProvider.put(pathWithLock.get1(), new Directory());

                if (parentWithLock != null) {
                    Directory parentDir = (Directory)storageProvider.get(parentWithLock.get1());
                    parentDir.getFiles().add(pathWithLock.get1());
                    storageProvider.put(parentWithLock.get1(), parentDir);
                    parentWithLock.get2().unlock();
                }

                parentWithLock = pathWithLock;
            }

            if (parentWithLock != null)
                parentWithLock.get2().unlock();
        }
        finally {
            for (IgniteBiTuple<String, Lock> pathWithLock : pathsToBeCreated)
                pathWithLock.get2().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Set<String> listFiles(String path) {
        Lock pathLock = storageProvider.lock(path);

        pathLock.lock();

        try {
            FileOrDirectory dir = storageProvider.get(path);

            // If directory doesn't exist throw an exception.
            if (dir == null)
                throw new IllegalStateException("Directory doesn't exist [path=" + path + "]");

            // If directory isn't a directory throw an exception.
            if (!dir.isDirectory())
                throw new IllegalStateException("Specified path is not associated with directory [path=" + path + "]");

            return ((Directory) dir).getFiles();
        }
        finally {
            pathLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(String path) {
        Lock pathLock = storageProvider.lock(path);

        pathLock.lock();

        try {
            FileOrDirectory file = storageProvider.get(path);
            storageProvider.remove(path);

            if (file.isDirectory()) {
                for (String s : ((Directory) file).getFiles())
                    remove(s);
            }
        }
        finally {
            pathLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean exists(String path) {
        return storageProvider.get(path) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectory(String path) {
        FileOrDirectory file = storageProvider.get(path);

        return file != null && file.isDirectory();
    }

    /** {@inheritDoc} */
    @Override public boolean isFile(String path) {
        FileOrDirectory file = storageProvider.get(path);

        return file != null && file.isFile();
    }

    /**
     * Returns parent directory for the specified path.
     *
     * @param path Path.
     * @return Parent directory path.
     */
    private String getParent(String path) {
        Path parentPath = Paths.get(path).getParent();
        return parentPath == null ? null : parentPath.toString();
    }
}
