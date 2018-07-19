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
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.tensorflow.cluster.TensorFlowJobArchive;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowServerAddressSpec;
import org.apache.ignite.tensorflow.core.util.CustomizableThreadFactory;
import org.apache.ignite.tensorflow.core.util.NativeProcessRunner;

/**
 * Utils class that helps to start and stop user script process.
 */
public class TensorFlowUserScriptRunner {
    /** User script process thread name. */
    private static final String USER_SCRIPT_PROCESS_THREAD_NAME = "tensorflow-user-script";

    /** User script folder prefix. */
    private static final String USER_SCRIPT_FOLDER_PREFIX = "tf_user_script_";

    /** Native process runner. */
    private static final NativeProcessRunner processRunner = new NativeProcessRunner();

    /** Logger. */
    private final IgniteLogger log;

    /** Job archive. */
    private final TensorFlowJobArchive jobArchive;

    /** TensorFlow cluster specification. */
    private final TensorFlowClusterSpec clusterSpec;

    /** Executors that is used to start and control native process. */
    private final ExecutorService executor;

    /** Working directory of the user script process. */
    private File workingDir;

    /** Future of the user script process. */
    private Future<?> fut;

    /** Callback. */
    private IgniteRunnable cb;

    /**
     * Constructs a new instance of TensorFlow user script runner.
     *
     * @param jobArchive Job archive.
     * @param clusterSpec TensorFlow cluster specification.
     */
    public TensorFlowUserScriptRunner(TensorFlowJobArchive jobArchive, TensorFlowClusterSpec clusterSpec, IgniteRunnable cb) {
        this.log = Ignition.ignite().log().getLogger(TensorFlowUserScriptRunner.class);
        this.jobArchive = jobArchive;
        this.clusterSpec = clusterSpec;
        this.cb = cb;
        this.executor = Executors.newSingleThreadExecutor(
            new CustomizableThreadFactory(USER_SCRIPT_PROCESS_THREAD_NAME, true)
        );
    }

    /**
     * Starts user script process.
     */
    public void start(String stdoutTopicName, String stderrTopicName) {
        log.info("Starting user script");

        try {
            workingDir = Files.createTempDirectory(USER_SCRIPT_FOLDER_PREFIX).toFile();
            unzip(jobArchive.getData(), workingDir);

            log.info("User script has been extracted into " + workingDir.getAbsolutePath());

            ProcessBuilder procBuilder = prepareProcessBuilder(workingDir);

            fut = executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        log.info("Starting user script native process in " + workingDir.getAbsolutePath());
                        processRunner.startAndWait(
                            procBuilder,
                            null,
                            str -> {
                                System.out.println(str);
                                Ignition.ignite().message().send(stdoutTopicName, str);
                            },
                            str -> {
                                System.err.println(str);
                                Ignition.ignite().message().send(stderrTopicName, str);
                            }
                        );
                        log.info("User script native process in " + workingDir.getAbsolutePath() +
                            " has been completed");
                        cb.run();
                        break;
                    }
                    catch (InterruptedException e) {
                        log.info("User script native process in " + workingDir.getAbsolutePath() +
                            " has been interrupted");
                        break;
                    }
                    catch (Exception e) {
                        log.error("User script native process failed", e);
                    }
                }
            });
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            if (workingDir != null) {
                delete(workingDir);
                log.info("Directory " + workingDir.getAbsolutePath() + " has been deleted");
            }

            throw e;
        }
    }

    /**
     * Stops user script process.
     */
    public void stop() {
        log.info("Stopping user script");

        if (fut != null && !fut.isDone())
            fut.cancel(true);

        if (workingDir != null) {
            delete(workingDir);
            log.info("Directory " + workingDir.getAbsolutePath() + " has been deleted");
        }
    }

    /**
     * Prepares process builder and specifies working directory and command to be run.
     *
     * @param workingDir Working directory.
     * @return Prepared process builder.
     */
    private ProcessBuilder prepareProcessBuilder(File workingDir) {
        ProcessBuilder procBuilder = new ProcessBuilder();

        procBuilder.directory(workingDir);
        procBuilder.command(jobArchive.getCommands());

        Map<String, String> env = procBuilder.environment();
        env.put("PYTHONPATH", workingDir.getAbsolutePath());
        env.put("TF_CONFIG", formatTfConfigVar());
        env.put("TF_WORKERS", formatTfWorkersVar());
        env.put("TF_CHIEF_SERVER", formatTfChiefServerVar());

        return procBuilder;
    }

    /**
     * Formats "TF_CONFIG" variable to be passed into user script.
     *
     * @return Formatted "TF_CONFIG" variable to be passed into user script.
     */
    private String formatTfConfigVar() {
        return new StringBuilder()
            .append("{\"cluster\" : ")
            .append(clusterSpec.format(Ignition.ignite()))
            .append(", ")
            .append("\"task\": {\"type\" : \"chief\", \"index\": 0}}")
            .toString();
    }

    /**
     * Formats "TF_WORKERS" variable to be passed into user script.
     *
     * @return Formatted "TF_WORKERS" variable to be passed into user script.
     */
    private String formatTfWorkersVar() {
        StringJoiner joiner = new StringJoiner(", ");

        int cnt = clusterSpec.getJobs().get("worker").size();
        for (int i = 0; i < cnt; i++)
            joiner.add("\"/job:worker/task:" + i + "\"");

        return "[" + joiner + "]";
    }

    /**
     * Formats "TF_CHIEF_SERVER" variable to be passed into user script.
     *
     * @return Formatted "TF_CHIEF_SERVER" variable to be passed into user script.
     */
    private String formatTfChiefServerVar() {
        TensorFlowServerAddressSpec spec = clusterSpec.getJobs().get("chief").get(0);
        return "grpc://" + spec.format(Ignition.ignite());
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
                throw new IllegalStateException("Can't delete directory [name=" + file.getAbsolutePath() + "]");
        }
        else {
            if (!file.delete())
                throw new IllegalStateException("Can't delete file [name=" + file.getAbsolutePath() + "]");
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
                        throw new IllegalStateException("Can't create directory [name=" + file.getAbsolutePath() + "]");
                }
                else {
                    if (!file.getParentFile().exists()) {
                        boolean created = file.getParentFile().mkdirs();
                        if (!created)
                            throw new IllegalStateException("Can't create directory [name=" +
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
