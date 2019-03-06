/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.jvmtest;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import javax.swing.JOptionPane;
import org.junit.Test;

/**
 * Java file locks test.
 */
public class FileLocksTest {
    /** File path (on Windows file will be created under the root directory of the current drive). */
    private static final String LOCK_FILE_PATH = "/test-java-file-lock-tmp.bin";

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
