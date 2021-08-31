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

package org.apache.ignite.internal.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.join;
import static java.lang.System.clearProperty;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static org.apache.ignite.internal.util.IgniteUtils.workDirectory;
import static org.apache.ignite.internal.util.typedef.internal.U.getIgniteHome;
import static org.apache.ignite.internal.util.typedef.internal.U.nullifyHomeDirectory;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** */
public class IgniteUtilsWorkDirectoryTest {
    /** */
    private static final String USER_WORK_DIR = join(File.separator,
        getIgniteHome(), "userWorkDirTest");

    /** */
    private static final String USER_IGNITE_HOME = join(File.separator,
        getIgniteHome(), "userIgniteHomeTest");

    /** */
    private static final String USER_DIR_PROPERTY_VALUE = join(File.separator,
        new File(getIgniteHome()).getParent(), "userDirPropertyTest");

    /** */
    private static String dfltIgniteHome;

    /** */
    private static String dfltUserDir;

    /** */
    @Before
    public void setup() {
        dfltIgniteHome = getProperty(IgniteSystemProperties.IGNITE_HOME);
        dfltUserDir = getProperty("user.dir");
        clearProperty(IgniteSystemProperties.IGNITE_HOME);
        clearProperty("user.dir");
    }

    /** */
    @After
    public void tearDown() {
        if (dfltIgniteHome != null)
            setProperty(IgniteSystemProperties.IGNITE_HOME, dfltIgniteHome);
        if (dfltUserDir != null)
            setProperty("user.dir", dfltUserDir);
    }

    /**
     * The work directory specified by the user has the highest priority
     */
    @Test
    public void testWorkDirectory1() {
        executeGenericTest(true, false, false,
            USER_WORK_DIR);
    }

    /**
     * The work directory specified by the user has the highest priority
     */
    @Test
    public void testWorkDirectory2() {
        executeGenericTest(true, false, true,
            USER_WORK_DIR);
    }

    /**
     * The work directory specified by the user has the highest priority
     */
    @Test
    public void testWorkDirectory3() {
        executeGenericTest(true, true, false,
            USER_WORK_DIR);
    }

    /**
     * The work directory specified by the user has the highest priority
     */
    @Test
    public void testWorkDirectory4() {
        executeGenericTest(true, true, true,
            USER_WORK_DIR);
    }

    /**
     * The method set/clear "user.dir" system property and invoke
     * {@link IgniteUtils#workDirectory(java.lang.String, java.lang.String)}
     * with ignite work directory and ignite home directory provided by user
     *
     * @param userWorkDirFlag need or not to pass userWorkDir to {@link IgniteUtils#workDirectory(java.lang.String, java.lang.String)}
     * @param userIgniteHomeFlag need or not to pass userIgniteHome to {@link IgniteUtils#workDirectory(java.lang.String, java.lang.String)}
     * @param userDirPropFlag need to set or clear "user.dir" system property
     * @param expWorkDir expected Ignite work directory that will be returned by
     * {@link IgniteUtils#workDirectory(java.lang.String, java.lang.String)}
     */
    private void executeGenericTest(boolean userWorkDirFlag, boolean userIgniteHomeFlag,
        boolean userDirPropFlag, String expWorkDir) {
        if (userDirPropFlag)
            setProperty("user.dir", USER_DIR_PROPERTY_VALUE);
        else
            clearProperty("user.dir");

        String userWorkDir = "";
        if (userWorkDirFlag)
            userWorkDir = USER_WORK_DIR;

        nullifyHomeDirectory();
        clearProperty(IgniteSystemProperties.IGNITE_HOME);
        String userIgniteHome = "";
        if (userIgniteHomeFlag)
            userIgniteHome = USER_IGNITE_HOME;

        String actualWorkDir = null;
        try {
            actualWorkDir = workDirectory(userWorkDir, userIgniteHome);
        }
        catch (Throwable e) {
            fail();
        }

        assertEquals(expWorkDir, actualWorkDir);

    }

    /** */
    @Test
    public void testNonAbsolutePathWorkDir() {
        genericPathExceptionTest("nonAbsolutePathTestDirectory",
            "Work directory path must be absolute: nonAbsolutePathTestDirectory");
    }

    /**
     * This test only makes sense on Linux platform.
     */
    @Test
    public void testDisabledWriteToWorkDir() {
        String strDir = join(File.separator, USER_WORK_DIR, "CannotWriteTestDirectory");
        File dir = new File(strDir);

        if (dir.exists()) {
            resetPermission(strDir);
            boolean deleted = U.delete(dir);
            assertTrue("cannot delete file", deleted);
        }

        dir.mkdirs();

        try {
            executeCommand("chmod 444 " + strDir);
            executeCommand("chattr +i " + strDir);

            genericPathExceptionTest(strDir, "Cannot write to work directory: " + strDir);
        }
        finally {
            resetPermission(strDir);
        }
    }

    /**
     * This test only makes sense on Linux platform.
     */
    @Test
    public void testDisabledWorkDirCreation() {
        String strDirParent = join(File.separator, USER_WORK_DIR, "CannotWriteTestDirectory");
        File dirParent = new File(strDirParent);

        if (dirParent.exists()) {
            resetPermission(strDirParent);
            boolean deleted = U.delete(dirParent);
            assertTrue("cannot delete file", deleted);
        }
        dirParent.mkdirs();

        try {
            executeCommand("chmod 444 " + strDirParent);
            executeCommand("chattr +i " + strDirParent);

            String strDir = join(File.separator, strDirParent, "newDirectory");

            genericPathExceptionTest(strDir, "Work directory does not exist and cannot be created: " + strDir);
        }
        finally {
            resetPermission(strDirParent);
        }
    }

    /** */
    private static void resetPermission(String dir) {
        executeCommand("chattr -i " + dir);
        executeCommand("chmod 777 " + dir);
    }

    /** */
    private static void executeCommand(String cmd) {
        X.println("Command to execute: " + cmd);

        try {
            Process proc = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));
            BufferedReader stdError = new BufferedReader(new
                InputStreamReader(proc.getErrorStream()));

            String s;

            while ((s = stdInput.readLine()) != null)
                X.println("stdInput: " + s);
            while ((s = stdError.readLine()) != null)
                X.println("stdError:" + s);
        }
        catch (Exception e) {
            fail();
        }
    }

    /** */
    private void genericPathExceptionTest(String userWorkDir, String expMsg) {
        assertThrows(null,
            () -> workDirectory(userWorkDir, null),
            IgniteCheckedException.class,
            expMsg
        );
    }

}
