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

package org.apache.ignite.ml.inference.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Utils class that helps to serialize directory content as a has map and then deserialize it.
 */
public class DirectorySerializer {
    /**
     * Serializes directory content.
     *
     * @param path Path to the directory.
     * @return Serialized directory content.
     * @throws IOException If directory cannot be serialized.
     */
    public static byte[] serialize(Path path) throws IOException {
        File file = path.toFile();

        if (!file.isDirectory())
            throw new IllegalStateException("Path is not directory [path=\"" + path + "\"]");

        Map<String, byte[]> data = new HashMap<>();
        serialize(data, path, file);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(data);
            oos.flush();

            return baos.toByteArray();
        }
    }

    /**
     * Deserializes directory content.
     *
     * @param path Path to the directory.
     * @param data Serialized content.
     * @throws IOException If the directory cannot be deserialized.
     * @throws ClassNotFoundException If the directory cannot be deserialized.
     */
    @SuppressWarnings("unchecked")
    public static void deserialize(Path path, byte[] data) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            Map<String, byte[]> files = (Map<String, byte[]>)ois.readObject();

            for (Map.Entry<String, byte[]> file : files.entrySet()) {
                Path dst = path.resolve(file.getKey());
                File dstFile = dst.toFile();
                Files.createDirectories(dstFile.getParentFile().toPath());
                Files.createFile(dstFile.toPath());
                try (FileOutputStream fos = new FileOutputStream(dstFile)) {
                    fos.write(file.getValue());
                    fos.flush();
                }
            }
        }
    }

    /**
     * Removes the specified directory.
     *
     * @param path Path to the directory.
     */
    public static void deleteDirectory(Path path) {
        File file = path.toFile();
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children)
                    deleteDirectory(child.toPath());
            }
        }

        try {
            Files.delete(path);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serializes directory content or file.
     *
     * @param data Storage to keep pairs of file name and file content.
     * @param basePath Base path to the serialized directory.
     * @param file File to be serialized.
     * @throws IOException If the file cannot be serialized.
     */
    private static void serialize(Map<String, byte[]> data, Path basePath, File file) throws IOException {
        if (file.isFile()) {
            String relative = basePath.relativize(file.toPath()).toString();
            byte[] bytes = Files.readAllBytes(file.toPath());
            data.put(relative, bytes);
        }
        else {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children)
                    serialize(data, basePath, child);
            }
        }
    }
}
