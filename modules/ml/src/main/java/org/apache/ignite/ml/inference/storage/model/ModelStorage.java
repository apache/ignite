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

import java.util.Set;

/**
 * Storage that allows to load, keep and get access to model in byte representation.
 */
public interface ModelStorage {
    /**
     * Creates a new or replaces existing file.
     *
     * @param path Path to file.
     * @param data File content.
     * @param onlyIfNotExist If file already exists throw an exception.
     */
    public void putFile(String path, byte[] data, boolean onlyIfNotExist);

    /**
     * Creates a new or replaces existing file.
     *
     * @param path Path to file.
     * @param data File content.
     */
    public default void putFile(String path, byte[] data) {
        putFile(path, data, false);
    }

    /**
     * Returns file content.
     *
     * @param path Path to file.
     * @return File content.
     */
    public byte[] getFile(String path);

    /**
     * Creates directory.
     *
     * @param path Path to directory.
     * @param onlyIfNotExist If directory already exists throw an exception.
     */
    public void mkdir(String path, boolean onlyIfNotExist);

    /**
     * Creates directory.
     *
     * @param path Path to directory.
     */
    public default void mkdir(String path) {
        mkdir(path, false);
    }

    /**
     * Creates directory and all required parent directories in the path.
     *
     * @param path Path to directory.
     */
    public void mkdirs(String path);

    /**
     * Returns list of files in the specified directory.
     *
     * @param path Path to directory.
     * @return List of files in the specified directory.
     */
    public Set<String> listFiles(String path);

    /**
     * Removes specified directory or file.
     *
     * @param path Path to directory or file.
     */
    public void remove(String path);

    /**
     * Returns {@code true} if a regular file or directory exist, otherwise {@code false}.
     *
     * @param path Path to directory or file.
     * @return {@code true} if a regular file or directory exist, otherwise {@code false}.
     */
    public boolean exists(String path);

    /**
     * Returns {@code true} if the specified path associated with a directory.
     *
     * @param path Path to directory or file.
     * @return {@code true} if the specified path associated with a directory.
     */
    public boolean isDirectory(String path);

    /**
     * Returns {@code true} if the specified path associated with a regular file.
     *
     * @param path Path to directory or file.
     * @return {@code true} if the specified path associated with a regular file.
     */
    public boolean isFile(String path);
}
