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

package org.apache.ignite.internal.processors.hadoop.impl.v2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.RunJar;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.hadoop.HadoopCommonUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 * Provides all resources are needed to the job execution. Downloads the main jar, the configuration and additional
 * files are needed to be placed on local files system.
 */
class HadoopV2JobResourceManager {
    /** File type Fs disable caching property name. */
    private static final String FILE_DISABLE_CACHING_PROPERTY_NAME =
        HadoopFileSystemsUtils.disableFsCachePropertyName("file");

    /** Hadoop job context. */
    private final JobContextImpl ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Job ID. */
    private final HadoopJobId jobId;

    /** Class path list. */
    private URL[] clsPath;

    /** Set of local resources. */
    private final Collection<File> rsrcSet = new HashSet<>();

    /** Staging directory to delivery job jar and config to the work nodes. */
    private Path stagingDir;

    /** The job. */
    private final HadoopV2Job job;

    /**
     * Creates new instance.
     * @param jobId Job ID.
     * @param ctx Hadoop job context.
     * @param log Logger.
     */
    public HadoopV2JobResourceManager(HadoopJobId jobId, JobContextImpl ctx, IgniteLogger log, HadoopV2Job job) {
        this.jobId = jobId;
        this.ctx = ctx;
        this.log = log.getLogger(HadoopV2JobResourceManager.class);
        this.job = job;
    }

    /**
     * Set working directory in local file system.
     *
     * @param dir Working directory.
     * @throws IOException If fails.
     */
    private void setLocalFSWorkingDirectory(File dir) throws IOException {
        JobConf cfg = ctx.getJobConf();

        ClassLoader oldLdr = HadoopCommonUtils.setContextClassLoader(cfg.getClassLoader());

        try {
            cfg.set(HadoopFileSystemsUtils.LOC_FS_WORK_DIR_PROP, dir.getAbsolutePath());

            if (!cfg.getBoolean(FILE_DISABLE_CACHING_PROPERTY_NAME, false))
                FileSystem.getLocal(cfg).setWorkingDirectory(new Path(dir.getAbsolutePath()));
        }
        finally {
            HadoopCommonUtils.restoreContextClassLoader(oldLdr);
        }
    }

    /**
     * Prepare job resources. Resolve the classpath list and download it if needed.
     *
     * @param download {@code true} If need to download resources.
     * @param jobLocDir Work directory for the job.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareJobEnvironment(boolean download, File jobLocDir) throws IgniteCheckedException {
        try {
            if (jobLocDir.exists())
                throw new IgniteCheckedException("Local job directory already exists: " + jobLocDir.getAbsolutePath());

            JobConf cfg = ctx.getJobConf();

            Collection<URL> clsPathUrls = new ArrayList<>();

            String mrDir = cfg.get(MRJobConfig.MAPREDUCE_JOB_DIR);

            if (mrDir != null) {
                stagingDir = new Path(new URI(mrDir));

                if (download) {
                    FileSystem fs = job.fileSystem(stagingDir.toUri(), cfg);

                    if (!fs.exists(stagingDir))
                        throw new IgniteCheckedException("Failed to find map-reduce submission " +
                            "directory (does not exist): " + stagingDir);

                    if (!FileUtil.copy(fs, stagingDir, jobLocDir, false, cfg))
                        throw new IgniteCheckedException("Failed to copy job submission directory "
                            + "contents to local file system "
                            + "[path=" + stagingDir + ", locDir=" + jobLocDir.getAbsolutePath()
                            + ", jobId=" + jobId + ']');
                }

                File jarJobFile = new File(jobLocDir, "job.jar");

                clsPathUrls.add(jarJobFile.toURI().toURL());

                rsrcSet.add(jarJobFile);
                rsrcSet.add(new File(jobLocDir, "job.xml"));
            }
            else if (!jobLocDir.mkdirs())
                throw new IgniteCheckedException("Failed to create local job directory: "
                    + jobLocDir.getAbsolutePath());

            processFiles(jobLocDir, ctx.getCacheFiles(), download, false, null, MRJobConfig.CACHE_LOCALFILES);
            processFiles(jobLocDir, ctx.getCacheArchives(), download, true, null, MRJobConfig.CACHE_LOCALARCHIVES);
            processFiles(jobLocDir, ctx.getFileClassPaths(), download, false, clsPathUrls, null);
            processFiles(jobLocDir, ctx.getArchiveClassPaths(), download, true, clsPathUrls, null);

            if (!clsPathUrls.isEmpty())
                clsPath = clsPathUrls.toArray(new URL[clsPathUrls.size()]);

            setLocalFSWorkingDirectory(jobLocDir);
        }
        catch (URISyntaxException | IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Process list of resources.
     *
     * @param jobLocDir Job working directory.
     * @param files Array of {@link URI} or {@link org.apache.hadoop.fs.Path} to process resources.
     * @param download {@code true}, if need to download. Process class path only else.
     * @param extract {@code true}, if need to extract archive.
     * @param clsPathUrls Collection to add resource as classpath resource.
     * @param rsrcNameProp Property for resource name array setting.
     * @throws IOException If failed.
     */
    private void processFiles(File jobLocDir, @Nullable Object[] files, boolean download, boolean extract,
        @Nullable Collection<URL> clsPathUrls, @Nullable String rsrcNameProp) throws IOException {
        if (F.isEmptyOrNulls(files))
            return;

        Collection<String> res = new ArrayList<>();

        for (Object pathObj : files) {
            Path srcPath;

            if (pathObj instanceof URI) {
                URI uri = (URI)pathObj;

                srcPath = new Path(uri);
            }
            else
                srcPath = (Path)pathObj;

            String locName = srcPath.getName();

            File dstPath = new File(jobLocDir.getAbsolutePath(), locName);

            res.add(locName);

            rsrcSet.add(dstPath);

            if (clsPathUrls != null)
                clsPathUrls.add(dstPath.toURI().toURL());

            if (!download)
                continue;

            JobConf cfg = ctx.getJobConf();

            FileSystem dstFs = FileSystem.getLocal(cfg);

            FileSystem srcFs = job.fileSystem(srcPath.toUri(), cfg);

            if (extract) {
                File archivesPath = new File(jobLocDir.getAbsolutePath(), ".cached-archives");

                if (!archivesPath.exists() && !archivesPath.mkdir())
                    throw new IOException("Failed to create directory " +
                        "[path=" + archivesPath + ", jobId=" + jobId + ']');

                File archiveFile = new File(archivesPath, locName);

                FileUtil.copy(srcFs, srcPath, dstFs, new Path(archiveFile.toString()), false, cfg);

                String archiveNameLC = archiveFile.getName().toLowerCase();

                if (archiveNameLC.endsWith(".jar"))
                    RunJar.unJar(archiveFile, dstPath);
                else if (archiveNameLC.endsWith(".zip"))
                    FileUtil.unZip(archiveFile, dstPath);
                else if (archiveNameLC.endsWith(".tar.gz") ||
                    archiveNameLC.endsWith(".tgz") ||
                    archiveNameLC.endsWith(".tar"))
                    FileUtil.unTar(archiveFile, dstPath);
                else
                    throw new IOException("Cannot unpack archive [path=" + srcPath + ", jobId=" + jobId + ']');
            }
            else
                FileUtil.copy(srcFs, srcPath, dstFs, new Path(dstPath.toString()), false, cfg);
        }

        if (!res.isEmpty() && rsrcNameProp != null)
            ctx.getJobConf().setStrings(rsrcNameProp, res.toArray(new String[res.size()]));
    }

    /**
     * Prepares working directory for the task.
     *
     * <ul>
     *     <li>Creates working directory.</li>
     *     <li>Creates symbolic links to all job resources in working directory.</li>
     * </ul>
     *
     * @param path Path to working directory of the task.
     * @throws IgniteCheckedException If fails.
     */
    public void prepareTaskWorkDir(File path) throws IgniteCheckedException {
        try {
            if (path.exists())
                throw new IOException("Task local directory already exists: " + path);

            if (!path.mkdir())
                throw new IOException("Failed to create directory: " + path);

            for (File resource : rsrcSet) {
                File symLink = new File(path, resource.getName());

                try {
                    Files.createSymbolicLink(symLink.toPath(), resource.toPath());
                }
                catch (IOException e) {
                    String msg = "Unable to create symlink \"" + symLink + "\" to \"" + resource + "\".";

                    if (U.isWindows() && e instanceof FileSystemException)
                        msg += "\n\nAbility to create symbolic links is required!\n" +
                                "On Windows platform you have to grant permission 'Create symbolic links'\n" +
                                "to your user or run the Accelerator as Administrator.\n";

                    throw new IOException(msg, e);
                }
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to prepare local working directory for the task " +
                 "[jobId=" + jobId + ", path=" + path+ ']', e);
        }
    }

    /**
     * Cleans up job staging directory.
     */
    public void cleanupStagingDirectory() {
        try {
            if (stagingDir != null) {
                FileSystem fs = job.fileSystem(stagingDir.toUri(), ctx.getJobConf());

                fs.delete(stagingDir, true);
            }
        }
        catch (Exception e) {
            log.error("Failed to remove job staging directory [path=" + stagingDir + ", jobId=" + jobId + ']' , e);
        }
    }

    /**
     * Returns array of class path for current job.
     *
     * @return Class path collection.
     */
    @Nullable public URL[] classPath() {
        return clsPath;
    }
}