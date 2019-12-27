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

import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
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
    @Override public void putFile(String path, byte[] data, boolean onlyIfNotExist) {
        String parentPath = getParent(path);

        // Paths are locked in child-first order.
        Lock pathLock = storageProvider.lock(path);
        Lock parentPathLock = storageProvider.lock(parentPath);

        synchronize(() -> {
            if (exists(path) && onlyIfNotExist)
                throw new IllegalArgumentException("File already exists [path=" + path + "]");

            FileOrDirectory parent = storageProvider.get(parentPath);

            // If parent doesn't exist throw an exception.
            if (parent == null)
                throw new IllegalArgumentException("Cannot create file because directory doesn't exist [path="
                    + path + "]");

            // If parent is not a directory throw an exception.
            if (!parent.isDirectory())
                throw new IllegalArgumentException("Cannot create file because parent is not a directory [path="
                    + path + "]");

            Directory dir = (Directory) parent;
            // Update parent if it's a new file.
            if (!dir.getFiles().contains(path)) {
                dir.getFiles().add(path);
                storageProvider.put(parentPath, parent);
            }

            // Save file into cache.
            storageProvider.put(path, new File(data));
        }, pathLock, parentPathLock);
    }

    /**  {@inheritDoc}*/
    @Override public byte[] getFile(String path) {
        FileOrDirectory fileOrDir = storageProvider.get(path);

        Lock pathLock = storageProvider.lock(path);

        return synchronize(() -> {
            // If file doesn't exist throw an exception.
            if (fileOrDir == null)
                throw new IllegalArgumentException("File doesn't exist [path=" + path + "]");

            // If file is not a regular file throw an exception.
            if (!fileOrDir.isFile())
                throw new IllegalArgumentException("File is not a regular file [path=" + path + "]");

            return ((File) fileOrDir).getData();
        }, pathLock);
    }

    /** {@inheritDoc} */
    @Override public void mkdir(String path, boolean onlyIfNotExist) {
        String parentPath = getParent(path);

        // Paths are locked in child-first order.
        Lock pathLock = storageProvider.lock(path);
        Lock parentPathLock = storageProvider.lock(parentPath);

        synchronize(() -> {
            // If a directory associated with specified path exists return.
            if (isDirectory(path)) {
                if (onlyIfNotExist)
                    throw new IllegalArgumentException("Directory already exists [path=" + path + "]");

                return;
            }

            // If a regular file associated with specified path exists throw an exception.
            if (isFile(path))
                throw new IllegalArgumentException("File with specified path already exists [path=" + path + "]");

            FileOrDirectory parent = storageProvider.get(parentPath);

            // If parent doesn't exist throw an exception.
            if (parent == null)
                throw new IllegalArgumentException("Cannot create directory because parent directory does not exist"
                    + " [path=" + path + "]");

            // If parent is not a directory throw an exception.
            if (!parent.isDirectory())
                throw new IllegalArgumentException("Cannot create directory because parent is not a directory [path="
                    + path + "]");

            Directory dir = (Directory) parent;
            dir.getFiles().add(path);

            // Update parent and save directory into cache.
            storageProvider.put(parentPath, parent);
            storageProvider.put(path, new Directory());
        }, pathLock, parentPathLock);
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

                    throw new IllegalArgumentException("Cannot create directory because parent is not a directory "
                        + "[path=" + path + "]");
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

        return synchronize(() -> {
            FileOrDirectory dir = storageProvider.get(path);

            // If directory doesn't exist throw an exception.
            if (dir == null)
                throw new IllegalArgumentException("Directory doesn't exist [path=" + path + "]");

            // If directory isn't a directory throw an exception.
            if (!dir.isDirectory())
                throw new IllegalArgumentException("Specified path is not associated with directory [path=" + path
                    + "]");

            return ((Directory) dir).getFiles();
        }, pathLock);
    }

    /** {@inheritDoc} */
    @Override public void remove(String path) {
        Lock pathLock = storageProvider.lock(path);

        synchronize(() -> {
            FileOrDirectory file = storageProvider.get(path);
            storageProvider.remove(path);

            if (file.isDirectory()) {
                for (String s : ((Directory) file).getFiles())
                    remove(s);
            }
        }, pathLock);
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
        String[] splittedPath = path.split("/");

        int cnt = 0;
        for (int i = 0; i < splittedPath.length; i++) {
            if (!splittedPath[i].isEmpty())
                cnt++;
        }

        if (cnt == 0)
            return null;

        StringBuilder parentPath = new StringBuilder("/");
        for (int i = 0; i < splittedPath.length; i++) {
            if (!splittedPath[i].isEmpty() && --cnt > 0)
                parentPath.append(splittedPath[i]).append("/");
        }

        if (parentPath.length() > 1)
            parentPath.delete(parentPath.length() - 1, parentPath.length());

        return parentPath.toString();
    }

    /**
     * Wraps task execution into locks.
     *
     * @param task Runnable task.
     * @param locks List of locks.
     */
    static void synchronize(Runnable task, Lock... locks) {
        synchronize(() -> {
            task.run();
            return null;
        }, locks);
    }

    /**
     * Wraps task execution into locks. Util method.
     *
     * @param task Task to executed.
     * @param locks List of locks.
     */
    static <T> T synchronize(Supplier<T> task, Lock... locks) {
        Throwable ex = null;
        T res;

        int i = 0;
        try {
            for (; i < locks.length; i++)
                locks[i].lock();

            res = task.get();
        }
        finally {
            for (i -= 1; i >= 0; i--) {
                try {
                    locks[i].unlock();
                }
                catch (RuntimeException | Error e) {
                    ex = e;
                }
            }
        }

        if (ex != null) {
            if (ex instanceof RuntimeException)
                throw (RuntimeException)ex;

            if (ex instanceof Error)
                throw (Error)ex;

            throw new IllegalStateException("Unexpected type of throwable");
        }

        return res;
    }
}
