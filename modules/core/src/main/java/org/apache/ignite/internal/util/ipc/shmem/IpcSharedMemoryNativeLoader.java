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

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.FileLock;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

/**
 * Shared memory native loader.
 */
@SuppressWarnings("ErrorNotRethrown")
public class IpcSharedMemoryNativeLoader {
    /** Library name base. */
    private static final String LIB_NAME_BASE = "igniteshmem";

    /** Library jar name base. */
    private static final String JAR_NAME_BASE = "shmem";

    /** Library name. */
    static final String LIB_NAME = LIB_NAME_BASE + "-" + VER_STR;

    /** Loaded flag. */
    private static volatile boolean loaded;

    /**
     * @return Operating system name to resolve path to library.
     */
    private static String os() {
        String name = System.getProperty("os.name").toLowerCase().trim();

        if (name.startsWith("win"))
            throw new IllegalStateException("IPC shared memory native loader should not be called on windows.");

        if (name.startsWith("linux"))
            return "linux";

        if (name.startsWith("mac os x"))
            return "osx";

        return name.replaceAll("\\W+", "_");
    }

    /**
     * @return Platform.
     */
    private static String platform() {
        return os() + bitModel();
    }

    /**
     * @return Bit model.
     */
    private static int bitModel() {
        String prop = System.getProperty("sun.arch.data.model");

        if (prop == null)
            prop = System.getProperty("com.ibm.vm.bitmode");

        if (prop != null)
            return Integer.parseInt(prop);

        // We don't know.
        return -1;
    }

    /**
     * @param log Logger, if available. If null, warnings will be printed out to console.
     * @throws IgniteCheckedException If failed.
     */
    public static void load(IgniteLogger log) throws IgniteCheckedException {
        if (loaded)
            return;

        synchronized (IpcSharedMemoryNativeLoader.class) {
            if (loaded)
                return;

            doLoad(log);

            loaded = true;
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private static void doLoad(IgniteLogger log) throws IgniteCheckedException {
        assert Thread.holdsLock(IpcSharedMemoryNativeLoader.class);

        Collection<Throwable> errs = new ArrayList<>();

        try {
            // Load native library (the library directory should be in java.library.path).
            System.loadLibrary(LIB_NAME);

            return;
        }
        catch (UnsatisfiedLinkError e) {
            errs.add(e);
        }

        File tmpDir = getUserSpecificTempDir();

        File lockFile = new File(tmpDir, "igniteshmem.lock");

        // Obtain lock on file to prevent concurrent extracts.
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(lockFile, "rws");
            FileLock ignored = randomAccessFile.getChannel().lock()) {
            if (extractAndLoad(errs, tmpDir, platformSpecificResourcePath()))
                return;

            if (extractAndLoad(errs, tmpDir, osSpecificResourcePath()))
                return;

            if (extractAndLoad(errs, tmpDir, resourcePath()))
                return;

            try {
                if (log != null)
                    LT.warn(log, null, "Failed to load 'igniteshmem' library from classpath. Will try to load it from IGNITE_HOME.");

                String igniteHome = X.resolveIgniteHome();

                File shmemJar = findShmemJar(errs, igniteHome);

                if (shmemJar != null) {
                    try (JarFile jar = new JarFile(shmemJar, false, JarFile.OPEN_READ)) {
                        if (extractAndLoad(errs, jar, tmpDir, platformSpecificResourcePath()))
                            return;

                        if (extractAndLoad(errs, jar, tmpDir, osSpecificResourcePath()))
                            return;

                        if (extractAndLoad(errs, jar, tmpDir, resourcePath()))
                            return;
                    }
                }
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }

            // Failed to find the library.
            assert !errs.isEmpty();

            throw new IgniteCheckedException("Failed to load native IPC library: " + errs);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to obtain file lock: " + lockFile, e);
        }
    }

    /**
     * Tries to find shmem jar in IGNITE_HOME/libs folder.
     *
     * @param errs Collection of errors to add readable exception to.
     * @param igniteHome Resolver IGNITE_HOME variable.
     * @return File, if found.
     */
    private static File findShmemJar(Collection<Throwable> errs, String igniteHome) {
        File libs = new File(igniteHome, "libs");

        if (!libs.exists() || libs.isFile()) {
            errs.add(new IllegalStateException("Failed to find libs folder in resolved IGNITE_HOME: " + igniteHome));

            return null;
        }

        for (File lib : libs.listFiles()) {
            if (lib.getName().endsWith(".jar") && lib.getName().contains(JAR_NAME_BASE))
                return lib;
        }

        errs.add(new IllegalStateException("Failed to find shmem jar in resolved IGNITE_HOME: " + igniteHome));

        return null;
    }

    /**
     * Gets temporary directory unique for each OS user.
     * The directory guaranteed to exist, though may not be empty.
     */
    private static File getUserSpecificTempDir() throws IgniteCheckedException {
        String tmp = System.getProperty("java.io.tmpdir");

        String userName = System.getProperty("user.name");

        File tmpDir = new File(tmp, userName);

        if (!tmpDir.exists())
            //noinspection ResultOfMethodCallIgnored
            tmpDir.mkdirs();

        if (!(tmpDir.exists() && tmpDir.isDirectory()))
            throw new IgniteCheckedException("Failed to create temporary directory [dir=" + tmpDir + ']');

        return tmpDir;
    }

    /**
     * @return OS resource path.
     */
    private static String osSpecificResourcePath() {
        return "META-INF/native/" + os() + "/" + mapLibraryName(LIB_NAME_BASE);
    }

    /**
     * @return Platform resource path.
     */
    private static String platformSpecificResourcePath() {
        return "META-INF/native/" + platform() + "/" + mapLibraryName(LIB_NAME_BASE);
    }

    /**
     * @return Resource path.
     */
    private static String resourcePath() {
        return "META-INF/native/" + mapLibraryName(LIB_NAME_BASE);
    }

    /**
     * @return Maps library name to file name.
     */
    private static String mapLibraryName(String name) {
        String libName = System.mapLibraryName(name);

        if (U.isMacOs() && libName.endsWith(".jnilib"))
            return libName.substring(0, libName.length() - "jnilib".length()) + "dylib";

        return libName;
    }

    /**
     * @param errs Errors collection.
     * @param rsrcPath Path.
     * @return {@code True} if library was found and loaded.
     */
    private static boolean extractAndLoad(Collection<Throwable> errs, File tmpDir, String rsrcPath) {
        ClassLoader clsLdr = U.detectClassLoader(IpcSharedMemoryNativeLoader.class);

        URL rsrc = clsLdr.getResource(rsrcPath);

        if (rsrc != null)
            return extract(errs, rsrc, new File(tmpDir, mapLibraryName(LIB_NAME)));
        else {
            errs.add(new IllegalStateException("Failed to find resource with specified class loader " +
                "[rsrc=" + rsrcPath + ", clsLdr=" + clsLdr + ']'));

            return false;
        }
    }

    /**
     * @param errs Errors collection.
     * @param rsrcPath Path.
     * @return {@code True} if library was found and loaded.
     */
    private static boolean extractAndLoad(Collection<Throwable> errs, JarFile jar, File tmpDir, String rsrcPath) {
        ZipEntry rsrc = jar.getEntry(rsrcPath);

        if (rsrc != null)
            return extract(errs, rsrc, jar, new File(tmpDir, mapLibraryName(LIB_NAME)));
        else {
            errs.add(new IllegalStateException("Failed to find resource within specified jar file " +
                "[rsrc=" + rsrcPath + ", jar=" + jar.getName() + ']'));

            return false;
        }
    }

    /**
     * @param errs Errors collection.
     * @param src Source.
     * @param target Target.
     * @return {@code True} if resource was found and loaded.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static boolean extract(Collection<Throwable> errs, URL src, File target) {
        FileOutputStream os = null;
        InputStream is = null;

        try {
            if (!target.exists() || !haveEqualMD5(target, src.openStream())) {
                is = src.openStream();

                if (is != null) {
                    os = new FileOutputStream(target);

                    int read;

                    byte[] buf = new byte[4096];

                    while ((read = is.read(buf)) != -1)
                        os.write(buf, 0, read);
                }
            }

            // chmod 775.
            if (!U.isWindows())
                Runtime.getRuntime().exec(new String[] {"chmod", "775", target.getCanonicalPath()}).waitFor();

            System.load(target.getPath());

            return true;
        }
        catch (IOException | UnsatisfiedLinkError | InterruptedException | NoSuchAlgorithmException e) {
            errs.add(e);
        }
        finally {
            U.closeQuiet(os);
            U.closeQuiet(is);
        }

        return false;
    }

    /**
     * @param errs Errors collection.
     * @param src Source.
     * @param target Target.
     * @return {@code True} if resource was found and loaded.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static boolean extract(Collection<Throwable> errs, ZipEntry src, JarFile jar, File target) {
        FileOutputStream os = null;
        InputStream is = null;

        try {
            if (!target.exists() || !haveEqualMD5(target, jar.getInputStream(src))) {
                is = jar.getInputStream(src);

                if (is != null) {
                    os = new FileOutputStream(target);

                    int read;

                    byte[] buf = new byte[4096];

                    while ((read = is.read(buf)) != -1)
                        os.write(buf, 0, read);
                }
            }

            // chmod 775.
            if (!U.isWindows())
                Runtime.getRuntime().exec(new String[] {"chmod", "775", target.getCanonicalPath()}).waitFor();

            System.load(target.getPath());

            return true;
        }
        catch (IOException | UnsatisfiedLinkError | InterruptedException | NoSuchAlgorithmException e) {
            errs.add(e);
        }
        finally {
            U.closeQuiet(os);
            U.closeQuiet(is);
        }

        return false;
    }

    /**
     * @param target Target.
     * @param srcIS Source input stream.
     * @return {@code True} if target md5-sum equal to source md5-sum.
     * @throws NoSuchAlgorithmException If md5 algorithm was not found.
     * @throws IOException If an I/O exception occurs.
     */
    private static boolean haveEqualMD5(File target, InputStream srcIS) throws NoSuchAlgorithmException, IOException {
        try {
            try (InputStream targetIS = new FileInputStream(target)) {
                String targetMD5 = U.calculateMD5(targetIS);
                String srcMD5 = U.calculateMD5(srcIS);

                return targetMD5.equals(srcMD5);
            }
        }
        finally {
            srcIS.close();
        }
    }
}