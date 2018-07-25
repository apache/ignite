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

package org.apache.ignite.tensorflow.submitter.command;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.cluster.TensorFlowClusterGatewayManager;
import org.apache.ignite.tensorflow.cluster.TensorFlowJobArchive;

/**
 * Start command that starts TensorFlow cluster for specified cache and runs specified job using this cluster.
 */
public class StartCommand implements Runnable {
    /** Ignite supplier. */
    private final Supplier<Ignite> igniteSupplier;

    /** Upstream cache name. */
    private final String upstreamCacheName;

    /** Job archive path. */
    private final String jobArchivePath;

    /** User command to be executed. */
    private final String[] commands;

    /**
     * Constructs a new instance of start command.
     *
     * @param igniteSupplier Ignite supplier.
     * @param upstreamCacheName Upstream cache name.
     * @param jobArchivePath Job archive path.
     * @param commands User command to be executed.
     */
    public StartCommand(Supplier<Ignite> igniteSupplier, String upstreamCacheName, String jobArchivePath,
        String[] commands) {
        this.igniteSupplier = igniteSupplier;
        this.upstreamCacheName = upstreamCacheName;
        this.jobArchivePath = jobArchivePath;
        this.commands = commands;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (Ignite ignite = igniteSupplier.get()) {
            UUID clusterId = UUID.randomUUID();
            TensorFlowJobArchive jobArchive = new TensorFlowJobArchive(
                upstreamCacheName,
                zip(jobArchivePath),
                commands
            );

            TensorFlowClusterGatewayManager mgr = new TensorFlowClusterGatewayManager(ignite);
            mgr.createCluster(clusterId, jobArchive);

            mgr.listenToClusterUserScript(clusterId, System.out::println, System.err::println);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Archives specified folder or file into zip archive.
     *
     * @param jobArchivePath Path to folder to be archived.
     * @return Byte array representing zip archive.
     * @throws IOException In case of input/output exception.
     */
    private byte[] zip(String jobArchivePath) throws IOException {
        Path path = Paths.get(jobArchivePath);
        File file = path.toFile();

        if (!file.exists())
            throw new IllegalArgumentException("File doesn't exist [name=" + jobArchivePath + "]");

        if (file.isDirectory())
            return zipDirectory(file);
        else if (jobArchivePath.endsWith(".zip"))
            return zipArchive(file);
        else
            return zipFile(file);
    }

    /**
     * Archives specified folder into zip archive.
     *
     * @param dir Directory to be archived.
     * @return Byte array representing zip archive.
     * @throws IOException In case of input/output exception.
     */
    private byte[] zipDirectory(File dir) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ZipOutputStream zipFile = new ZipOutputStream(baos)) {
            compressDirectoryToZip(dir.getAbsolutePath(), dir.getAbsolutePath(), zipFile);
        }

        return baos.toByteArray();
    }

    /**
     * Archives specified file into zip archive.
     *
     * @param file File to be archived.
     * @return Byte array representing zip archive.
     * @throws IOException In case of input/output exception.
     */
    private byte[] zipFile(File file) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry(file.getName());
            zos.putNextEntry(entry);

            try (FileInputStream in = new FileInputStream(file.getAbsolutePath())) {
                IOUtils.copy(in, zos);
            }
        }

        return baos.toByteArray();
    }

    /**
     * Reads zip archive into byte array and returns this array.
     *
     * @param file Archive to be read.
     * @return Byte array representing zip archive.
     * @throws IOException In case of input/output exception.
     */
    private byte[] zipArchive(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            return IOUtils.toByteArray(fis);
        }
    }

    /**
     * Archives specified folder into zip output stream.
     *
     * @param rootDir Root directory.
     * @param srcDir Source directory.
     * @param out Zip output stream.
     * @throws IOException In case of input/output exception.
     */
    private void compressDirectoryToZip(String rootDir, String srcDir, ZipOutputStream out) throws IOException {
        File[] files = new File(srcDir).listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory())
                    compressDirectoryToZip(rootDir, srcDir + File.separator + file.getName(), out);
                else {
                    ZipEntry entry = new ZipEntry(srcDir.replace(rootDir, "")
                        + File.separator + file.getName());
                    out.putNextEntry(entry);

                    try (FileInputStream in = new FileInputStream(srcDir + File.separator + file.getName())) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        }
    }

    /** */
    public Supplier<Ignite> getIgniteSupplier() {
        return igniteSupplier;
    }

    /** */
    public String getUpstreamCacheName() {
        return upstreamCacheName;
    }

    /** */
    public String getJobArchivePath() {
        return jobArchivePath;
    }

    /** */
    public String[] getCommands() {
        return commands;
    }
}
