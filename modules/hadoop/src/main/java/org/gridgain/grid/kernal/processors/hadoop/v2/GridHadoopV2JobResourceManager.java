/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class GridHadoopV2JobResourceManager {
    /** Job config. */
    private JobConf cfg;

    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Directory to place localized resources. */
    private File jobLocDir;

    /** Class path list. */
    private List<URL> clsPath = new ArrayList<>();

    /**
     * Provides all resources are needed to the job execution. Downloads the main jar, the configuration and additional
     * files are needed to be placed on local files system.
     *
     * @param jobId Job ID.
     * @param cfg Job config.
     * @param jobLocDir Directory to place resources.
     */
    public GridHadoopV2JobResourceManager(GridHadoopJobId jobId, JobConf cfg, File jobLocDir) {
        this.jobId = jobId;
        this.cfg = cfg;
        this.jobLocDir = jobLocDir;
    }

    /**
     * Prepare job resources. Resolve the classpath list and download it if needed.
     *
     * @throws GridException If failed.
     */
    public void processJobResources(boolean download) throws GridException {
        try {
            String mrDir = cfg.get("mapreduce.job.dir");

            if (mrDir != null) {
                if (download) {
                    Path path = new Path(new URI(mrDir));

                    FileSystem fs = FileSystem.get(path.toUri(), cfg);

                    if (!fs.exists(path))
                        throw new GridException("Failed to find map-reduce submission directory (does not exist): " +
                                path);

                    if (jobLocDir.exists())
                        throw new GridException("Local job directory already exists: " + jobLocDir.getAbsolutePath());

                    if (!FileUtil.copy(fs, path, jobLocDir, false, cfg))
                        throw new GridException("Failed to copy job submission directory contents to local file system " +
                                "[path=" + path + ", locDir=" + jobLocDir.getAbsolutePath() + ", jobId=" + jobId + ']');
                }

                clsPath.add(new File(jobLocDir, "job.jar").toURI().toURL());

                processFiles(DistributedCache.getCacheFiles(cfg), download, false, false);

                processFiles(DistributedCache.getCacheArchives(cfg), download, true, false);

                processFiles(DistributedCache.getFileClassPaths(cfg), download, false, true);

                processFiles(DistributedCache.getArchiveClassPaths(cfg), download, true, true);
            }
        }
        catch (URISyntaxException | IOException e) {
            throw new GridException(e);
        }
    }

    /**
     * Process list of resources.
     *
     * @param files Array of {@link URI} or {@link Path} to process resources.
     * @param download {@code true}, if need to download. Process class path only else.
     * @param extract {@code true}, if need to extract archive.
     * @param addToClsPath {@code true}, if need to add the resource to class path.
     * @throws IOException If errors.
     */
    private void processFiles(@Nullable Object[] files, boolean download, boolean extract, boolean addToClsPath)
        throws IOException {
        if (files==null)
            return;

        for (Object pathObj : files) {
            String locName = null;
            Path srcPath;

            if (pathObj instanceof URI) {
                URI uri = (URI)pathObj;

                locName = uri.getFragment();

                srcPath = new Path(uri);
            }
            else
                srcPath = (Path)pathObj;

            if (locName == null)
                locName = srcPath.getName();

            File dstPath = new File(jobLocDir.getAbsolutePath(), locName);

            if (addToClsPath)
                clsPath.add(dstPath.toURI().toURL());

            if (!download)
                return;

            FileSystem dstFs = FileSystem.getLocal(cfg);

            //TODO: Replace this to the correct working directory redefinition.
            dstFs.setWorkingDirectory(new Path(jobLocDir.getAbsolutePath()));

            FileSystem srcFs = srcPath.getFileSystem(cfg);

            if (extract) {
                File archivesPath = new File(jobLocDir.getAbsolutePath(), ".cached-archives");

                if (!archivesPath.exists() && !archivesPath.mkdir())
                    throw new IOException("Failed to create directory " +
                         "[path=" + archivesPath + ", jobId=" + jobId + ']');

                File archiveFile = new File(archivesPath, locName);

                FileUtil.copy(srcFs, srcPath, dstFs, new Path(archiveFile.toString()), false, cfg);

                String archiveNameLC = archiveFile.getName().toLowerCase();

                if (archiveNameLC.endsWith(".jar")) {
                    RunJar.unJar(archiveFile, dstPath);
                }
                else if (archiveNameLC.endsWith(".zip")) {
                    FileUtil.unZip(archiveFile, dstPath);
                }
                else if (archiveNameLC.endsWith(".tar.gz") ||
                        archiveNameLC.endsWith(".tgz") ||
                        archiveNameLC.endsWith(".tar")) {
                    FileUtil.unTar(archiveFile, dstPath);
                }
                else {
                    throw new IOException("Cannot unpack archive [path=" + srcPath + ", jobId=" + jobId + ']');
                }
            }
            else
                FileUtil.copy(srcFs, srcPath, dstFs, new Path(dstPath.toString()), false, cfg);
        }
    }

    /**
     * Class path list.
     */
    public List<URL> getClassPath() {
        return clsPath;
    }

}
