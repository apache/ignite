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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.security.*;
import java.util.*;

import static org.apache.ignite.internal.IgniteVersionUtils.*;

/**
 * Shared memory native loader.
 */
@SuppressWarnings("ErrorNotRethrown")
public class IpcSharedMemoryNativeLoader {
    /** Loaded flag. */
    private static volatile boolean loaded;

    /** Library name base. */
    private static final String LIB_NAME_BASE = "igniteshmem";

    /** Lock file path. */
    private static final File LOCK_FILE = new File(System.getProperty("java.io.tmpdir"), "igniteshmem.lock");

    /** Library name. */
    static final String LIB_NAME = LIB_NAME_BASE + "-" + VER_STR;

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
     * @throws IgniteCheckedException If failed.
     */
    public static void load() throws IgniteCheckedException {
        if (loaded)
            return;

        synchronized (IpcSharedMemoryNativeLoader.class) {
            if (loaded)
                return;

            doLoad();

            loaded = true;
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private static void doLoad() throws IgniteCheckedException {
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

        // Obtain lock on file to prevent concurrent extracts.
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(LOCK_FILE, "rws");
             FileLock ignored = randomAccessFile.getChannel().lock()) {
            if (extractAndLoad(errs, platformSpecificResourcePath()))
                return;

            if (extractAndLoad(errs, osSpecificResourcePath()))
                return;

            if (extractAndLoad(errs, resourcePath()))
                return;

            // Failed to find the library.
            assert !errs.isEmpty();

            throw new IgniteCheckedException("Failed to load native IPC library: " + errs);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to obtain file lock: " + LOCK_FILE, e);
        }
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
    private static boolean extractAndLoad(Collection<Throwable> errs, String rsrcPath) {
        ClassLoader clsLdr = U.detectClassLoader(IpcSharedMemoryNativeLoader.class);

        URL rsrc = clsLdr.getResource(rsrcPath);

        if (rsrc != null)
            return extract(errs, rsrc, new File(System.getProperty("java.io.tmpdir"), mapLibraryName(LIB_NAME)));
        else {
            errs.add(new IllegalStateException("Failed to find resource with specified class loader " +
                "[rsrc=" + rsrcPath + ", clsLdr=" + clsLdr + ']'));

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
            if (!target.exists() || !haveEqualMD5(target, src)) {
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
     * @param target Target.
     * @param src Source.
     * @return {@code True} if target md5-sum equal to source md5-sum.
     * @throws NoSuchAlgorithmException If md5 algorithm was not found.
     * @throws IOException If an I/O exception occurs.
     */
    private static boolean haveEqualMD5(File target, URL src) throws NoSuchAlgorithmException, IOException {
        try (InputStream targetIS = new FileInputStream(target);
             InputStream srcIS = src.openStream()) {

            String targetMD5 = U.calculateMD5(targetIS);
            String srcMD5 = U.calculateMD5(srcIS);

            return targetMD5.equals(srcMD5);
        }
    }
}
