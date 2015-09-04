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

package org.apache.ignite.jvmtest;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import javax.swing.JOptionPane;
import junit.framework.TestCase;

/**
 * Java file locks test.
 */
public class FileLocksTest extends TestCase {
    /** File path (on Windows file will be created under the root directory of the current drive). */
    private static final String LOCK_FILE_PATH = "/test-java-file-lock-tmp.bin";

    /**
     * @throws Exception If failed.
     */
    public void testWriteLocks() throws Exception {
        final File file = new File(LOCK_FILE_PATH);

        file.createNewFile();

        RandomAccessFile raf = new RandomAccessFile(file, "rw");

        System.out.println("Getting lock...");

        FileLock lock = raf.getChannel().lock();

        System.out.println("Obtained lock: " + lock);

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    RandomAccessFile raf = new RandomAccessFile(file, "rw");

                    System.out.println("Getting lock (parallel thread)...");

                    FileLock lock = raf.getChannel().lock();

                    System.out.println("Obtained lock (parallel tread): " + lock);

                    lock.release();
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        thread.start();

        JOptionPane.showMessageDialog(null, "Press OK to release lock.");

        lock.release();

        thread.join();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadLocks() throws Exception {
        final File file = new File(LOCK_FILE_PATH);

        file.createNewFile();

        RandomAccessFile raf = new RandomAccessFile(file, "r");

        System.out.println("Getting lock...");

        FileLock lock = raf.getChannel().lock(0, Long.MAX_VALUE, true);

        System.out.println("Obtained lock: " + lock);

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    RandomAccessFile raf = new RandomAccessFile(file, "r");

                    System.out.println("Getting lock (parallel thread)...");

                    FileLock lock = raf.getChannel().lock(0, Long.MAX_VALUE, true);

                    System.out.println("Obtained lock (parallel thread): " + lock);

                    lock.release();

                }
                catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        thread.start();

        JOptionPane.showMessageDialog(null, "Press OK to release lock.");

        lock.release();

        thread.join();
    }
}