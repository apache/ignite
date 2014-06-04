/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;

/**
 * Task context shared by all tasks during execution.
 * Contains class loader required for hadoop job class loading.
 */
public class GridHadoopJobClassLoadingContext {
    /** Job class loader. */
    private ClassLoaderWrapper jobLdr;

    /** Local node ID. */
    private UUID locNodeId;

    /** Hadoop job. */
    private GridHadoopJob job;

    /** Output base dir. */
    private File outBase;

    /** Logger. */
    private GridLogger log;

    /**
     * Initializes context.
     *
     * @param job Job for which context is created.
     */
    public GridHadoopJobClassLoadingContext(UUID locNodeId, GridHadoopJob job, GridLogger log) {
        this.locNodeId = locNodeId;
        this.job = job;
        this.log = log.getLogger(GridHadoopJobClassLoadingContext.class);
    }

    /**
     * Gets job class loader.
     *
     * @return Job class loader.
     */
    public ClassLoader jobClassLoader() {
        return jobLdr == null ? getClass().getClassLoader() : jobLdr;
    }

    /**
     * Initializes class loader.
     *
     * @throws GridException
     */
    public void initializeClassLoader() throws GridException {
        try {
            outBase = U.resolveWorkDirectory("hadoop", false);

            long start = System.currentTimeMillis();

            final Collection<URL> jars = new ArrayList<>();

            File dir = jobJarsFolder(job.id(), locNodeId);

            if (!dir.exists())
                return;

            Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<java.nio.file.Path>() {
                @Override public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
                    throws IOException {
                    if (file.getFileName().toString().endsWith(".jar")) {
                        String jar = file.normalize().toAbsolutePath().toString();

                        if (log.isDebugEnabled())
                            log.debug("Adding to job context classpath [jobId=" + job.id() + ", jar=" + jar + ']');

                        jars.add(file.toUri().toURL());
                    }

                    return super.visitFile(file, attrs);
                }
            });

            URL[] urls = new URL[jars.size()];

            jars.toArray(urls);

            final URLClassLoader urlLdr = new URLClassLoader(urls);

            jobLdr = new ClassLoaderWrapper(urlLdr, getClass().getClassLoader());

            long end = System.currentTimeMillis();

            if (log.isDebugEnabled())
                log.debug("Task shared context class loader initialized in: " + (end - start));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    public void destroy() {
        if (jobLdr != null)
            jobLdr.destroy();
    }

    /**
     * Prepares class loader for task execution and returns previous context class loader.
     *
     * @param ctx Context, if exists.
     * @param info Info to prepare.
     * @return Previous context class loader.
     */
    public static ClassLoader prepareClassLoader(@Nullable GridHadoopJobClassLoadingContext ctx,
        GridHadoopJobInfo info) {
        ClassLoader old = Thread.currentThread().getContextClassLoader();

        if (ctx == null)
            return old;

        Thread.currentThread().setContextClassLoader(ctx.jobClassLoader());

        ((GridHadoopDefaultJobInfo)info).configuration().setClassLoader(
            ctx.jobClassLoader());

        return old;
    }

    /**
     * Prepares job files.
     *
     * @throws GridException If failed.
     */
    public void prepareJobFiles() throws GridException {
        try {
            outBase = U.resolveWorkDirectory("hadoop", false);

            String mrDir = job.property("mapreduce.job.dir");

            if (mrDir != null) {
                long start = System.currentTimeMillis();

                Path path = new Path(new URI(mrDir));

                JobConf cfg = ((GridHadoopDefaultJobInfo)job.info()).configuration();

                FileSystem fs = FileSystem.get(path.toUri(), cfg);

                if (!fs.exists(path))
                    throw new GridException("Failed to find map-reduce submission directory (does not exist): " +
                        path);

                File dir = jobJarsFolder(job.id(), locNodeId);

                FileUtil.fullyDeleteContents(dir);

                if (log.isDebugEnabled())
                    log.debug("Copying job submission directory to local file system " +
                        "[path=" + path + ", locDir=" + dir.getAbsolutePath() + ", jobId=" + job.id() + ']');

                if (!FileUtil.copy(fs, path, dir, false, cfg))
                    throw new GridException("Failed to copy job submission directory contents to local file system " +
                        "[path=" + path + ", locDir=" + dir.getAbsolutePath() + ", jobId=" + job.id() + ']');

                long end = System.currentTimeMillis();

                if (log.isDebugEnabled())
                    log.debug("Job files deployed to local file system in: " + (end - start));
            }
        }
        catch (URISyntaxException | IOException e) {
            throw new GridException(e);
        }
    }

    /**
     * @param jobId Job ID.
     * @return Job jars dir.
     */
    private File jobJarsFolder(GridHadoopJobId jobId, UUID locNodeId) {
        File workFldr = new File(outBase, "Job_" + jobId);

        return new File(workFldr, "jars-" + locNodeId);
    }

    /**
     *
     */
    private static class ClassLoaderWrapper extends ClassLoader implements GridInternalClassLoader {
        /** */
        private volatile URLClassLoader delegate;

        /**
         * Makes classes available for GC.
         */
        public void destroy() {
            delegate = null;
        }

        /**
         * @param delegate Delegate.
         */
        private ClassLoaderWrapper(URLClassLoader delegate, ClassLoader parent) {
            super(parent);

            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (delegate != null) {
                try {
                    return delegate.loadClass(name);
                } catch (ClassNotFoundException ignore) {
                    return super.loadClass(name);
                }
            }
            else
                return super.loadClass(name);
        }

        /** {@inheritDoc} */
        @Override public InputStream getResourceAsStream(String name) {
            return delegate != null ? delegate.getResourceAsStream(name) : super.getResourceAsStream(name);
        }

        /** {@inheritDoc} */
        @Override public URL findResource(final String name) {
            return delegate != null ? delegate.findResource(name) : super.findResource(name);
        }

        /** {@inheritDoc} */
        @Override public Enumeration<URL> findResources(final String name) throws IOException {
            return delegate != null ? delegate.findResources(name) : super.findResources(name);
        }
    }
}
