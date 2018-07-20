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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.cluster.TensorFlowCluster;
import org.apache.ignite.tensorflow.cluster.TensorFlowClusterGateway;
import org.apache.ignite.tensorflow.cluster.TensorFlowClusterGatewayManager;
import org.apache.ignite.tensorflow.cluster.TensorFlowJobArchive;

public class StartCommand implements Command {

    private final String upstreamCacheName;

    private final String jobArchivePath;

    private final String[] commands;

    public StartCommand(String upstreamCacheName, String jobArchivePath, String[] commands) {
        this.upstreamCacheName = upstreamCacheName;
        this.jobArchivePath = jobArchivePath;
        this.commands = commands;
    }

    /** {@inheritDoc} */
    @Override public void runWithinIgnite(Ignite ignite) {
        try {
            UUID clusterId = UUID.randomUUID();
            TensorFlowJobArchive jobArchive = new TensorFlowJobArchive(upstreamCacheName, zip(jobArchivePath), commands);

            TensorFlowClusterGatewayManager mgr = new TensorFlowClusterGatewayManager(ignite);
            TensorFlowClusterGateway gateway = mgr.getOrCreateCluster(clusterId, jobArchive);

            ignite.message().localListen("us_out_" + clusterId, (node, msg) -> {
                System.out.println(msg);
                return true;
            });

            ignite.message().localListen("us_err_" + clusterId, (node, msg) -> {
                System.err.println(msg);
                return true;
            });

            CountDownLatch latch = new CountDownLatch(1);

            Consumer<Optional<TensorFlowCluster>> subscriber = cluster -> {
                if (!cluster.isPresent())
                    latch.countDown();
            };

            gateway.subscribe(subscriber);
            latch.await();
            gateway.unsubscribe(subscriber);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

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

    private byte[] zipDirectory(File file) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ZipOutputStream zipFile = new ZipOutputStream(baos)) {
            compressDirectoryToZip(file.getAbsolutePath(), file.getAbsolutePath(), zipFile);
        }

        return baos.toByteArray();
    }

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

    private byte[] zipArchive(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            return IOUtils.toByteArray(fis);
        }
    }

    private void compressDirectoryToZip(String rootDir, String srcDir, ZipOutputStream out) throws IOException {
        File[] files = new File(srcDir).listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory())
                    compressDirectoryToZip(rootDir, srcDir + File.separator + file.getName(), out);
                else {
                    ZipEntry entry = new ZipEntry(srcDir.replace(rootDir, "") + File.separator + file.getName());
                    out.putNextEntry(entry);

                    try (FileInputStream in = new FileInputStream(srcDir + File.separator + file.getName())) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        }
    }
}
