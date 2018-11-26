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

/**
 * Default implementation of {@link ModelStorage} that can use any {@link ModelStorageProvider} as a backend storage
 * system.
 */
public class DefaultModelStorage implements ModelStorage {
    /** Ignite Cache that is used to store model storage files. */
    private final ModelStorageProvider storageProvider;

//    /**
//     * Returns Ignite model storage that uses Ignite cache to store models.
//     *
//     * @param ignite Ignite instance.
//     * @return Ignite model storage.
//     */
//    public static DefaultModelStorage getModelStorage(ModelStorageProvider storageProvider) {
//        IgniteCache<String, FileOrDirectory> cache = ignite.cache(MODEL_STORAGE_CACHE_NAME);
//
//        return new DefaultModelStorage(cache);
//    }

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
        storageProvider.lock(path);
        storageProvider.lock(parentPath);

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
            storageProvider.unlock(parentPath);
            storageProvider.unlock(path);
        }
    }

    /**  {@inheritDoc}*/
    @Override public byte[] getFile(String path) {
        FileOrDirectory fileOrDir = storageProvider.get(path);

        storageProvider.lock(path);

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
            storageProvider.unlock(path);
        }
    }

    @Override public void mkdir(String path) {
        String parentPath = getParent(path);

        // Paths are locked in child-first order.
        storageProvider.lock(path);
        storageProvider.lock(parentPath);

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
            storageProvider.unlock(path);
            storageProvider.unlock(parentPath);
        }
    }

    @Override public void mkdirs(String path) {
        Deque<String> pathsToBeCreated = new LinkedList<>();

        try {
            while (path != null) {
                // Paths are locked in child-first order.
                storageProvider.lock(path);

                if (exists(path)) {
                    if (isDirectory(path))
                        break;

                    throw new IllegalStateException("Cannot create directory because parent is not a directory [path="
                        + path + "]");
                }

                pathsToBeCreated.push(path);
                path = getParent(path);
            }

            String parent = path;
            while (!pathsToBeCreated.isEmpty()) {
                path = pathsToBeCreated.pop();

                storageProvider.put(path, new Directory());

                if (parent != null) {
                    Directory parentDir = (Directory)storageProvider.get(parent);
                    parentDir.getFiles().add(path);
                    storageProvider.put(parent, parentDir);
                    storageProvider.unlock(parent);
                }

                parent = path;
            }
        }
        finally {
            for (String p : pathsToBeCreated)
                storageProvider.unlock(p);
        }
    }

    @Override public Set<String> listFiles(String path) {
        storageProvider.lock(path);

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
            storageProvider.unlock(path);
        }
    }

    @Override public void remove(String path) {
        storageProvider.lock(path);

        try {
            FileOrDirectory file = storageProvider.get(path);
            storageProvider.remove(path);

            if (file.isDirectory()) {
                for (String s : ((Directory) file).getFiles())
                    remove(s);
            }
        }
        finally {
            storageProvider.unlock(path);
        }
    }

    @Override public boolean exists(String path) {
        return storageProvider.get(path) != null;
    }

    @Override public boolean isDirectory(String path) {
        FileOrDirectory file = storageProvider.get(path);

        return file != null && file.isDirectory();
    }

    @Override public boolean isFile(String path) {
        FileOrDirectory file = storageProvider.get(path);

        return file != null && file.isFile();
    }

    private String getParent(String path) {
        Path parentPath = Paths.get(path).getParent();
        return parentPath == null ? null : parentPath.toString();
    }
}
