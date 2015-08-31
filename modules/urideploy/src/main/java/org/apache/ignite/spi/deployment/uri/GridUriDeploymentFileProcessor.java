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

package org.apache.ignite.spi.deployment.uri;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.spi.deployment.uri.UriDeploymentSpi.XML_DESCRIPTOR_PATH;

/**
 * Utility class.
 * <p>
 * Provides useful and common functions for URI deployment.
 */
final class GridUriDeploymentFileProcessor {
    /**
     * Enforces singleton.
     */
    private GridUriDeploymentFileProcessor() {
        // No-op.
    }

    /**
     * Method processes given GAR file and extracts all tasks from it which are
     * either mentioned in GAR descriptor or implements interface {@link org.apache.ignite.compute.ComputeTask}
     * if there is no descriptor in file.
     *
     * @param file GAR file with tasks.
     * @param uri GAR file deployment URI.
     * @param deployDir deployment directory with downloaded GAR files.
     * @param log Logger.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if file could not be read.
     * @return List of tasks from given file.
     */
    @Nullable static GridUriDeploymentFileProcessorResult processFile(File file, String uri, File deployDir,
        IgniteLogger log) throws IgniteSpiException {
        File gar = file;

        if (!checkIntegrity(file, log)) {
            U.error(log, "Tasks in GAR not loaded in configuration (invalid file signature) [uri=" +
                U.hidePassword(uri) + ']');

            return null;
        }

        if (!file.isDirectory()) {
            gar = new File(deployDir, "dirzip_" + file.getName());

            gar.mkdirs();

            try {
                U.unzip(file, gar, log);
            }
            catch (IOException e) {
                throw new IgniteSpiException("IO error when unzipping GAR file: " + file.getAbsolutePath(), e);
            }
        }

        GridUriDeploymentFileProcessorResult res = null;

        if (gar.isDirectory()) {
            try {
                File xml = new File(gar, XML_DESCRIPTOR_PATH);

                if (!xml.exists() || xml.isDirectory()) {
                    U.warn(log,
                        "Processing deployment without descriptor file (it will cause full classpath scan) [path="
                        + XML_DESCRIPTOR_PATH + ", gar=" + gar.getAbsolutePath() + ']');

                    res = processNoDescriptorFile(gar, uri, log);
                }
                else {
                    InputStream in = null;

                    try {
                        in = new BufferedInputStream(new FileInputStream(xml));

                        // Parse XML task definitions and add them to cache.
                        GridUriDeploymentSpringDocument doc = GridUriDeploymentSpringParser.parseTasksDocument(in, log);

                        assert doc != null;

                        res = processWithDescriptorFile(doc, gar, uri, log);
                    }
                    finally {
                        U.close(in, log);
                    }
                }
            }
            catch (IOException e) {
                throw new IgniteSpiException("IO error when parsing GAR directory: " + gar.getAbsolutePath(), e);
            }
        }

        if (res != null)
            res.setMd5(md5(gar, log));

        return res;
    }

    /**
     * Calculates md5 checksum for the given file o directory.
     * For directories tries to walk all nested files accumulating the result.
     *
     * @param file file to calculate sum or root directory for accumulating calculation.
     * @param log logger to log all failures.
     * @return string representation of the calculated checksum or {@code null} if calculation failed.
     */
    @Nullable public static String md5(@Nullable File file, @Nullable IgniteLogger log) {
        if (file != null)
            return file.isFile() ? fileMd5(file, log) : directoryMd5(file, log);

        return null;
    }

    /**
     * Calculates md5 checksum for the given file
     *
     * @param file file to calculate md5.
     * @param log logger to log all failures.
     * @return string representation of the calculated checksum or {@code null} if calculation failed.
     */
    @Nullable public static String fileMd5(@Nullable File file, @Nullable IgniteLogger log) {
        String md5 = null;

        if (file != null) {
            if (!file.isFile()) {
                U.warn(log, "Failed to find file for md5 calculation: " + file);

                return null;
            }

            InputStream in = null;

            try {
                in = new BufferedInputStream(new FileInputStream(file));

                md5 = DigestUtils.md5Hex(in);
            }
            catch (IOException e) {
                U.warn(log, "Failed to open input stream for md5 calculation: " + e.getMessage());
            }
            finally {
                U.closeQuiet(in);
            }
        }

        return md5;
    }

    /**
     * For directories tries to walk all nested files accumulating them into single md5 checksum.
     *
     * @param dir directory to calculate md5.
     * @param log logger to log all failures.
     * @return string representation of the calculated checksum or {@code null} if calculation failed.
     */
    @Nullable public static String directoryMd5(@Nullable File dir, @Nullable IgniteLogger log) {
        if (dir != null) {
            if (!dir.isDirectory()) {
                U.warn(log, "Failed to find directory for md5 calculation: " + dir);

                return null;
            }

            try {
                MessageDigest digest = MessageDigest.getInstance("MD5");

                return addDirectoryDigest(dir, digest, log) ? Hex.encodeHexString(digest.digest()) : null;
            }
            catch (NoSuchAlgorithmException e) {
                throw new IgniteException("MD5 digest algorithm not found.", e);
            }
        }

        return null;
    }

    /**
     * Repulsively adds all files in the given directory to the given Digest object.
     *
     * @param file directory to start calculation from.
     * @param digest digest object where all available files should be applied.
     * @param log logger to report errors.
     * @return {@code true} if digest was added successfully, {@code false} otherwise.
     */
    private static boolean addDirectoryDigest(File file, MessageDigest digest, @Nullable IgniteLogger log) {
        assert file.isDirectory();

        File[] files = file.listFiles();

        if (files == null)
            return true;

        for (File visited : files) {
            if (visited.isFile()) {
                if (!addFileDigest(visited, digest, log))
                    return false;
            }
            else if (visited.isDirectory()) {
                if (!addDirectoryDigest(visited, digest, log))
                    return false;
            }
        }

        return true;
    }

    /**
     * Adds given file to the given Digest object.
     *
     * @param file file for digest calculations.
     * @param digest digest object to add file.
     * @param log logger to report errors.
     * @return {@code true} if digest was added successfully, {@code false} otherwise.
     */
    private static boolean addFileDigest(File file, MessageDigest digest, @Nullable IgniteLogger log) {
        if (!file.isFile()) {
            U.error(log, "Failed to add file to directory digest (will not check MD5 hash): " + file);

            return false;
        }

        InputStream in = null;

        try {
            in = new BufferedInputStream(new FileInputStream(file));

            byte[] buf = new byte[1024];

            int read = in.read(buf, 0, 1024);

            while (read > -1) {
                digest.update(buf, 0, read);

                read = in.read(buf, 0, 1024);
            }
        }
        catch (IOException e) {
            U.error(log, "Failed to add file to directory digest (will not check MD5 hash): " + file, e);

            return false;
        }
        finally {
            U.closeQuiet(in);
        }

        return true;
    }

    /**
     * Cleanup class loaders resource.
     *
     * @param clsLdr Released class loader.
     * @param log Logger.
     */
    static void cleanupUnit(ClassLoader clsLdr, IgniteLogger log) {
        assert clsLdr != null;
        assert log != null;

        if (clsLdr instanceof URLClassLoader) {
            URLClassLoader clsLdr0 = (URLClassLoader)clsLdr;

            U.close(clsLdr0, log);

            try {
                URL url = clsLdr0.getURLs()[0];

                File dir = new File(url.toURI());

                U.delete(dir);

                if (dir.getName().startsWith("dirzip_")) {
                    File jarFile = new File(dir.getParentFile(), dir.getName().substring(7));

                    U.delete(jarFile);
                }
            }
            catch (Exception e) {
                U.error(log, "Failed to cleanup unit [clsLdr=" + clsLdr + ']', e);
            }
        }
    }

    /**
     * Processes given GAR file and returns back all tasks which are in
     * descriptor.
     *
     * @param doc GAR file descriptor.
     * @param file GAR file.
     * @param uri GAR file deployment URI.
     * @param log Logger.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if it's impossible to open file.
     * @return List of tasks from descriptor.
     */
    @SuppressWarnings({"ClassLoader2Instantiation"})
    private static GridUriDeploymentFileProcessorResult processWithDescriptorFile(GridUriDeploymentSpringDocument doc,
        File file, String uri, IgniteLogger log) throws IgniteSpiException {
        ClassLoader clsLdr = GridUriDeploymentClassLoaderFactory.create(U.gridClassLoader(), file, log);

        List<Class<? extends ComputeTask<?, ?>>> tasks = doc.getTasks(clsLdr);

        List<Class<? extends ComputeTask<?, ?>>> validTasks = null;

        if (!F.isEmpty(tasks)) {
            validTasks = new ArrayList<>();

            for (Class<? extends ComputeTask<?, ?>> task : tasks) {
                if (!isAllowedTaskClass(task)) {
                    U.warn(log, "Failed to load task. Task should be public none-abstract class " +
                        "(might be inner static one) that implements GridComputeTask interface [taskCls=" + task + ']');
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Found grid deployment task: " + task.getName());

                    validTasks.add(task);
                }
            }
        }

        GridUriDeploymentFileProcessorResult res = new GridUriDeploymentFileProcessorResult();

        res.setFile(file);
        res.setClassLoader(clsLdr);

        if (!F.isEmpty(validTasks))
            res.setTaskClasses(validTasks);
        else if (log.isDebugEnabled())
            log.debug("No tasks loaded from file [file=" + file.getAbsolutePath() +
                ", uri=" + U.hidePassword(uri) + ']');

        return res;
    }

    /**
     * Processes GAR files which have no descriptor. It scans every class and
     * checks if it is a valid task or not. All valid tasks are returned back.
     *
     * @param file GAR file or directory.
     * @param uri GAR file deployment URI.
     * @param log Logger.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if file reading error happened.
     * @return List of tasks from given file.
     */
    private static GridUriDeploymentFileProcessorResult processNoDescriptorFile(File file, String uri, IgniteLogger log)
        throws IgniteSpiException {
        ClassLoader clsLdr = GridUriDeploymentClassLoaderFactory.create(U.gridClassLoader(), file, log);

        Set<Class<? extends ComputeTask<?, ?>>> clss = GridUriDeploymentDiscovery.getClasses(clsLdr, file);

        GridUriDeploymentFileProcessorResult res = new GridUriDeploymentFileProcessorResult();

        res.setFile(file);
        res.setClassLoader(clsLdr);

        if (clss != null) {
            List<Class<? extends ComputeTask<?, ?>>> validTasks =
                new ArrayList<>(clss.size());

            for (Class<? extends ComputeTask<?, ?>> cls : clss) {
                if (isAllowedTaskClass(cls)) {
                    if (log.isDebugEnabled())
                        log.debug("Found grid deployment task: " + cls.getName());

                    validTasks.add(cls);
                }
            }

            if (!validTasks.isEmpty())
                res.setTaskClasses(validTasks);
            else if (log.isDebugEnabled())
                log.debug("No tasks loaded from file [file=" + file.getAbsolutePath() +
                    ", uri=" + U.hidePassword(uri) + ']');
        }

        return res;
    }

    /**
     * Check that class may be instantiated as {@link org.apache.ignite.compute.ComputeTask} and used
     * in deployment.
     *
     * Loaded task class must implement interface {@link org.apache.ignite.compute.ComputeTask}.
     * Only non-abstract, non-interfaces and public classes allowed.
     * Inner static classes also allowed for loading.
     *
     * @param cls Class to check
     * @return {@code true} if class allowed for deployment.
     */
    private static boolean isAllowedTaskClass(Class<?> cls) {
        if (!ComputeTask.class.isAssignableFrom(cls))
            return false;

        int modifiers = cls.getModifiers();

        return !Modifier.isAbstract(modifiers) && !Modifier.isInterface(modifiers) &&
            (!cls.isMemberClass() || Modifier.isStatic(modifiers)) && Modifier.isPublic(modifiers);
    }

    /**
     * Make integrity check for GAR file.
     * Method returns {@code false} if GAR file has incorrect signature.
     *
     * @param file GAR file which should be verified.
     * @param log Logger.
     * @return {@code true} if given file is a directory of verification
     *      completed successfully otherwise returns {@code false}.
     */
    private static boolean checkIntegrity(File file, IgniteLogger log) {
        try {
            return file.isDirectory() || GridUriDeploymentJarVerifier.verify(file.getAbsolutePath(), false, log);
        }
        catch (IOException e) {
            U.error(log, "Error while making integrity file check.", e);
        }

        return false;
    }
}