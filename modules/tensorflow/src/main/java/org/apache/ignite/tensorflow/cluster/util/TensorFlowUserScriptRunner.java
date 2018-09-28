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

package org.apache.ignite.tensorflow.cluster.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.tensorflow.cluster.TensorFlowJobArchive;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowServerAddressSpec;
import org.apache.ignite.tensorflow.core.util.AsyncNativeProcessRunner;
import org.apache.ignite.tensorflow.core.util.NativeProcessRunner;

/**
 * Utils class that helps to start and stop user script process.
 */
public class TensorFlowUserScriptRunner extends AsyncNativeProcessRunner {
    /** Ignite logger. */
    private final IgniteLogger log;

    /** Job archive that will be extracted and used as working directory for the native process. */
    private final TensorFlowJobArchive jobArchive;

    /** TensorFlow cluster specification. */
    private final TensorFlowClusterSpec clusterSpec;

    /** Output stream data consumer. */
    private final Consumer<String> out;

    /** Error stream data consumer. */
    private final Consumer<String> err;

    /** Working directory of the user script process. */
    private File workingDir;

    /**
     * Constructs a new instance of TensorFlow user script runner.
     *
     * @param ignite Ignite instance.
     * @param executor Executor to be used in {@link AsyncNativeProcessRunner}.
     * @param jobArchive  Job archive that will be extracted and used as working directory for the native process.
     * @param clusterSpec TensorFlow cluster specification.
     * @param out Output stream data consumer.
     * @param err  Error stream data consumer.
     */
    public TensorFlowUserScriptRunner(Ignite ignite, ExecutorService executor, TensorFlowJobArchive jobArchive,
        TensorFlowClusterSpec clusterSpec, Consumer<String> out, Consumer<String> err) {
        super(ignite, executor);

        this.log = ignite.log().getLogger(TensorFlowUserScriptRunner.class);

        this.jobArchive = jobArchive;
        this.clusterSpec = clusterSpec;
        this.out = out;
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public NativeProcessRunner doBefore() {
        try {
            workingDir = Files.createTempDirectory("tf_us_").toFile();
            log.debug("Directory has been created [path=" + workingDir.getAbsolutePath() + "]");

            unzip(jobArchive.getData(), workingDir);
            log.debug("Job archive has been extracted [path=" + workingDir.getAbsolutePath() + "]");

            return prepareNativeProcessRunner();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void doAfter() {
        if (workingDir != null) {
            delete(workingDir);
            log.debug("Directory has been deleted [path=" + workingDir.getAbsolutePath() + "]");
        }
    }

    /**
     * Prepares process builder and specifies working directory and command to be run.
     *
     * @return Prepared process builder.
     */
    private NativeProcessRunner prepareNativeProcessRunner() {
        if (workingDir == null)
            throw new IllegalStateException("Working directory is not created");

        ProcessBuilder procBuilder = new TensorFlowProcessBuilderSupplier(false, null).get();

        procBuilder.directory(workingDir);
        procBuilder.command(jobArchive.getCommands());

        Map<String, String> env = procBuilder.environment();
        env.put("PYTHONPATH", workingDir.getAbsolutePath());
        env.put("TF_CLUSTER", formatTfClusterVar());
        env.put("TF_WORKERS", formatTfWorkersVar());
        env.put("TF_CHIEF_SERVER", formatTfChiefServerVar());

        return new NativeProcessRunner(procBuilder, null, out, err);
    }

    /**
     * Formats "TF_CLUSTER" variable to be passed into user script.
     *
     * @return Formatted "TF_CLUSTER" variable to be passed into user script.
     */
    private String formatTfClusterVar() {
        return clusterSpec.format(Ignition.ignite());
    }

    /**
     * Formats "TF_WORKERS" variable to be passed into user script.
     *
     * @return Formatted "TF_WORKERS" variable to be passed into user script.
     */
    private String formatTfWorkersVar() {
        StringJoiner joiner = new StringJoiner(", ");

        int cnt = clusterSpec.getJobs().get(TensorFlowClusterResolver.WORKER_JOB_NAME).size();
        for (int i = 0; i < cnt; i++)
            joiner.add("\"/job:" + TensorFlowClusterResolver.WORKER_JOB_NAME + "/task:" + i + "\"");

        return "[" + joiner + "]";
    }

    /**
     * Formats "TF_CHIEF_SERVER" variable to be passed into user script.
     *
     * @return Formatted "TF_CHIEF_SERVER" variable to be passed into user script.
     */
    private String formatTfChiefServerVar() {
        List<TensorFlowServerAddressSpec> tasks = clusterSpec.getJobs().get(TensorFlowClusterResolver.CHIEF_JOB_NAME);

        if (tasks == null || tasks.size() != 1)
            throw new IllegalStateException("TensorFlow cluster specification should contain exactly one chief task");

        TensorFlowServerAddressSpec addrSpec = tasks.iterator().next();

        return "grpc://" + addrSpec.format(Ignition.ignite());
    }

    /**
     * Clears given file or directory recursively.
     *
     * @param file File or directory to be cleaned,
     */
    private void delete(File file) {
        if (file.isDirectory()) {
            String[] files = file.list();

            if (files != null && files.length != 0)
                for (String fileToBeDeleted : files)
                    delete(new File(file, fileToBeDeleted));

            if (!file.delete())
                throw new IllegalStateException("Can't delete directory [path=" + file.getAbsolutePath() + "]");
        }
        else {
            if (!file.delete())
                throw new IllegalStateException("Can't delete file [path=" + file.getAbsolutePath() + "]");
        }
    }

    /**
     * Extracts specified zip archive into specified directory.
     *
     * @param data Zip archive to be extracted.
     * @param extractTo Target directory.
     */
    private void unzip(byte[] data, File extractTo) {
        try (ZipInputStream zipStream = new ZipInputStream(new ByteArrayInputStream(data))) {
            ZipEntry entry;
            while ((entry = zipStream.getNextEntry()) != null) {
                File file = new File(extractTo, entry.getName());

                if (entry.isDirectory() && !file.exists()) {
                    boolean created = file.mkdirs();
                    if (!created)
                        throw new IllegalStateException("Can't create directory [path=" + file.getAbsolutePath() + "]");
                }
                else {
                    if (!file.getParentFile().exists()) {
                        boolean created = file.getParentFile().mkdirs();
                        if (!created)
                            throw new IllegalStateException("Can't create directory [path=" +
                                file.getParentFile().getAbsolutePath() + "]");
                    }

                    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
                        IOUtils.copy(zipStream, out);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
