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

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteFs.*;
import static org.apache.ignite.fs.IgniteFsMode.*;

/**
 * Tests for GGFS working in mode when remote file system exists: DUAL_SYNC, DUAL_ASYNC.
 */
public abstract class GridGgfsDualAbstractSelfTest extends GridGgfsAbstractSelfTest {
    /**
     * Constructor.
     *
     * @param mode GGFS mode.
     */
    protected GridGgfsDualAbstractSelfTest(IgniteFsMode mode) {
        super(mode);

        assert mode == DUAL_SYNC || mode == DUAL_ASYNC;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultDirectories() throws Exception {
        IgniteFsPath gg = new IgniteFsPath("/gridgain");
        IgniteFsPath[] paths = paths(
            gg, new IgniteFsPath(gg, "sync"), new IgniteFsPath(gg, "async"), new IgniteFsPath(gg, "primary"));

        create(ggfs, paths, null);

        for (IgniteFsPath p : paths)
            assert ggfs.exists(p);

        assert ggfs.listFiles(gg).size() == 3;
        assert ggfs.listPaths(gg).size() == 3;
    }

    /**
     * Test existence check when the path exists only remotely.
     *
     * @throws Exception If failed.
     */
    public void testExistsPathMissing() throws Exception {
        create(ggfsSecondary, paths(DIR), null);

        assert ggfs.exists(DIR);
    }

    /**
     * Test list files routine when the path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    public void testListFilesPathMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        Collection<IgniteFsFile> paths = ggfs.listFiles(SUBDIR);

        assert paths != null;
        assert paths.size() == 2;

        Iterator<IgniteFsFile> iter = paths.iterator();

        IgniteFsFile path1 = iter.next();
        IgniteFsFile path2 = iter.next();

        assert (SUBSUBDIR.equals(path1.path()) && FILE.equals(path2.path())) ||
            (FILE.equals(path1.path()) && SUBSUBDIR.equals(path2.path()));
    }

    /**
     * Test info routine when the path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    public void testInfoPathMissing() throws Exception {
        create(ggfsSecondary, paths(DIR), null);
        create(ggfs, null, null);

        IgniteFsFile info = ggfs.info(DIR);

        assert info != null;

        assertEquals(DIR, info.path());
    }

    /**
     * Test rename in case source exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testRenameFileSourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, paths(DIR), null);

        ggfs.rename(FILE, FILE2);

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, FILE2);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test rename in case source doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testRenameFileSourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, null, null);

        ggfs.rename(FILE, FILE2);

        checkExist(ggfs, DIR, SUBDIR);
        checkExist(ggfs, ggfsSecondary, FILE2);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test file rename when parent folder is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    public void testRenameFileParentRootSourceMissing() throws Exception {
        IgniteFsPath file1 = new IgniteFsPath("/file1");
        IgniteFsPath file2 = new IgniteFsPath("/file2");

        create(ggfsSecondary, null, paths(file1));
        create(ggfs, null, null);

        ggfs.rename(file1, file2);

        checkExist(ggfs, ggfsSecondary, file2);
        checkNotExist(ggfs, ggfsSecondary, file1);
    }

    /**
     * Test rename in case source exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testRenameDirectorySourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, paths(DIR), null);

        ggfs.rename(SUBDIR, SUBDIR2);

        checkExist(ggfs, ggfsSecondary, SUBDIR2);
        checkNotExist(ggfs, ggfsSecondary, SUBDIR);
    }

    /**
     * Test rename in case source doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testRenameDirectorySourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, null, null);

        ggfs.rename(SUBDIR, SUBDIR2);

        checkExist(ggfs, DIR);
        checkExist(ggfs, ggfsSecondary, SUBDIR2);
        checkNotExist(ggfs, ggfsSecondary, SUBDIR);
    }

    /**
     * Test directory rename when parent folder is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    public void testRenameDirectoryParentRootSourceMissing() throws Exception {
        IgniteFsPath dir1 = new IgniteFsPath("/dir1");
        IgniteFsPath dir2 = new IgniteFsPath("/dir2");

        create(ggfsSecondary, paths(dir1), null);
        create(ggfs, null, null);

        ggfs.rename(dir1, dir2);

        checkExist(ggfs, ggfsSecondary, dir2);
        checkNotExist(ggfs, ggfsSecondary, dir1);
    }

    /**
     * Test move in case source exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        ggfs.rename(FILE, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move in case source doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), paths(FILE));

        ggfs.rename(FILE, SUBDIR_NEW);

        checkExist(ggfs, DIR, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move in case destination exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, SUBDIR, DIR_NEW), paths(FILE));

        ggfs.rename(FILE, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move in case destination doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, SUBDIR), paths(FILE));

        ggfs.rename(FILE, SUBDIR_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move in case source and destination exist partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceAndDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, DIR_NEW), null);

        ggfs.rename(FILE, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move in case source and destination don't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceAndDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, null, null);

        ggfs.rename(FILE, SUBDIR_NEW);

        checkExist(ggfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test file move when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileDestinationRootSourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, paths(DIR), null);

        ggfs.rename(FILE, new IgniteFsPath());

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath("/" + FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test file move when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileDestinationRootSourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, null, null);

        ggfs.rename(FILE, new IgniteFsPath());

        checkExist(ggfs, DIR, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath("/" + FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test file move when source parent is the root and the source is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceParentRootSourceMissing() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(file, SUBDIR_NEW);

        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, file);
    }

    /**
     * Test file move when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceParentRootDestinationMissingPartially() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(ggfs, paths(DIR_NEW), null);

        ggfs.rename(file, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, file);
    }

    /**
     * Test file move when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceParentRootDestinationMissing() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(ggfs, null, null);

        ggfs.rename(file, SUBDIR_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(ggfs, ggfsSecondary, file);
    }

    /**
     * Test move and rename in case source exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        ggfs.rename(FILE, FILE_NEW);

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move and rename in case source doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), paths(FILE));

        ggfs.rename(FILE, FILE_NEW);

        checkExist(ggfs, DIR, SUBDIR);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move and rename in case destination exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, SUBDIR, DIR_NEW), paths(FILE));

        ggfs.rename(FILE, FILE_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move and rename in case destination doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, SUBDIR), paths(FILE));

        ggfs.rename(FILE, FILE_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move and rename in case source and destination exist partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceAndDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, paths(DIR, DIR_NEW), null);

        ggfs.rename(FILE, FILE_NEW);

        checkExist(ggfs, SUBDIR, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test move and rename in case source and destination don't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceAndDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(ggfs, null, null);

        ggfs.rename(FILE, FILE_NEW);

        checkExist(ggfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test file move and rename when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileDestinationRootSourceMissingPartially() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE.name());

        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, paths(DIR), null);

        ggfs.rename(FILE, file);

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, file);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test file move and rename when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileDestinationRootSourceMissing() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE.name());

        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, null, null);

        ggfs.rename(FILE, file);

        checkExist(ggfs, DIR, SUBDIR);
        checkExist(ggfs, ggfsSecondary, file);
        checkNotExist(ggfs, ggfsSecondary, FILE);
    }

    /**
     * Test file move and rename when source parent is the root and the source is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceParentRootSourceMissing() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE_NEW.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(file, FILE_NEW);

        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, file);
    }

    /**
     * Test file move and rename when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceParentRootDestinationMissingPartially() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE_NEW.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(ggfs, paths(DIR_NEW), null);

        ggfs.rename(file, FILE_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, file);
    }

    /**
     * Test file move and rename when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceParentRootDestinationMissing() throws Exception {
        IgniteFsPath file = new IgniteFsPath("/" + FILE_NEW.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(ggfs, null, null);

        ggfs.rename(file, FILE_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, FILE_NEW);
        checkNotExist(ggfs, ggfsSecondary, file);
    }

    /**
     * Test move in case source exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case source doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(ggfs, DIR);
        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case destination exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectoryDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case destination doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectoryDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, SUBDIR, SUBSUBDIR), null);

        ggfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case source and destination exist partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceAndDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, DIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case source and destination don't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceAndDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, null, null);

        ggfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(ggfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory  move when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectoryDestinationRootSourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(ggfs, paths(DIR), null);

        ggfs.rename(SUBSUBDIR, new IgniteFsPath());

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath("/" + SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectoryDestinationRootSourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(ggfs, null, null);

        ggfs.rename(SUBSUBDIR, new IgniteFsPath());

        checkExist(ggfs, DIR, SUBDIR);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath("/" + SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when source parent is the root and the source folder is missing locally.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceParentRootSourceMissing() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(dir, SUBDIR_NEW);

        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, dir);
    }

    /**
     * Test directory move when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceParentRootDestinationMissingPartially() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(ggfs, paths(DIR_NEW), null);

        ggfs.rename(dir, SUBDIR_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, dir);
    }

    /**
     * Test directory move when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceParentRootDestinationMissing() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(ggfs, null, null);

        ggfs.rename(dir, SUBDIR_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, new IgniteFsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(ggfs, ggfsSecondary, dir);
    }

    /**
     * Test move and rename in case source exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case source doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(ggfs, DIR);
        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case destination exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectoryDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case destination doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectoryDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, SUBDIR, SUBSUBDIR), null);

        ggfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case source and destination exist partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceAndDestinationMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, paths(DIR, DIR_NEW), null);

        ggfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(ggfs, SUBDIR, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case source and destination don't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceAndDestinationMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(ggfs, null, null);

        ggfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(ggfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectoryDestinationRootSourceMissingPartially() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR.name());

        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(ggfs, paths(DIR), null);

        ggfs.rename(SUBSUBDIR, dir);

        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, dir);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectoryDestinationRootSourceMissing() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR.name());

        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(ggfs, null, null);

        ggfs.rename(SUBSUBDIR, dir);

        checkExist(ggfs, DIR, SUBDIR);
        checkExist(ggfs, ggfsSecondary, dir);
        checkNotExist(ggfs, ggfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when source parent is the root and the source is missing locally.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceParentRootSourceMissing() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR_NEW.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(ggfs, paths(DIR_NEW, SUBDIR_NEW), null);

        ggfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, dir);
    }

    /**
     * Test directory move and rename when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceParentRootDestinationMissingPartially() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR_NEW.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(ggfs, paths(DIR_NEW), null);

        ggfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(ggfs, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, dir);
    }

    /**
     * Test directory move and rename when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceParentRootDestinationMissing() throws Exception {
        IgniteFsPath dir = new IgniteFsPath("/" + SUBSUBDIR_NEW.name());

        create(ggfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(ggfs, null, null);

        ggfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(ggfs, DIR_NEW, SUBDIR_NEW);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(ggfs, ggfsSecondary, dir);
    }

    /**
     * Test mkdirs in case parent exists remotely, but some part of the parent path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    public void testMkdirsParentPathMissingPartially() throws Exception {
        Map<String, String> props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, paths(DIR), null);

        ggfs.mkdirs(SUBSUBDIR, props);

        // Ensure that directory was created and properties are propagated.
        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR);

        assertEquals(props, ggfsSecondary.info(SUBSUBDIR).properties());

        // We check only permission because GGFS client adds username and group name explicitly.
        assertEquals(props.get(PROP_PERMISSION), ggfs.info(SUBSUBDIR).properties().get(PROP_PERMISSION));
    }

    /**
     * Test mkdirs in case parent exists remotely, but no parents exist locally.
     *
     * @throws Exception If failed.
     */
    public void testMkdrisParentPathMissing() throws Exception {
        Map<String, String> props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, null, null);

        ggfs.mkdirs(SUBSUBDIR, props);

        // Ensure that directory was created and properties are propagated.
        checkExist(ggfs, DIR);
        checkExist(ggfs, SUBDIR);
        checkExist(ggfs, ggfsSecondary, SUBSUBDIR);

        assertEquals(props, ggfsSecondary.info(SUBSUBDIR).properties());

        // We check only permission because GGFS client adds username and group name explicitly.
        assertEquals(props.get(PROP_PERMISSION), ggfs.info(SUBSUBDIR).properties().get(PROP_PERMISSION));
    }

    /**
     * Test delete in case parent exists remotely, but some part of the parent path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    public void testDeletePathMissingPartially() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        create(ggfs, paths(DIR), null);

        ggfs.delete(SUBDIR, true);

        checkExist(ggfs, DIR);
        checkNotExist(ggfs, ggfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test delete in case parent exists remotely, but no parents exist locally.
     *
     * @throws Exception If failed.
     */
    public void testDeletePathMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        create(ggfs, null, null);

        ggfs.delete(SUBDIR, true);

        checkExist(ggfs, DIR);
        checkNotExist(ggfs, ggfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test delete when the path parent is the root and the path is missing locally.
     *
     * @throws Exception If failed.
     */
    public void testDeleteParentRootPathMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        create(ggfs, null, null);

        ggfs.delete(DIR, true);

        checkNotExist(ggfs, ggfsSecondary, DIR, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test update in case file exists remotely, but some part of the path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    public void testUpdatePathMissingPartially() throws Exception {
        Map<String, String> propsSubDir = properties("subDirOwner", "subDirGroup", "0555");
        Map<String, String> propsFile = properties("fileOwner", "fileGroup", "0666");

        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, paths(DIR), null);

        // Set different properties to the sub-directory.
        ggfsSecondary.update(SUBDIR, propsSubDir);

        ggfs.update(FILE, propsFile);

        // Ensure missing entries were re-created locally.
        checkExist(ggfs, SUBDIR, FILE);

        // Ensure properties propagation.
        assertEquals(propsSubDir, ggfsSecondary.info(SUBDIR).properties());
        assertEquals(propsSubDir, ggfs.info(SUBDIR).properties());

        assertEquals(propsFile, ggfsSecondary.info(FILE).properties());
        assertEquals(propsFile, ggfs.info(FILE).properties());
    }

    /**
     * Test update in case file exists remotely, but neither the file nor all it's parents exist locally.
     *
     * @throws Exception If failed.
     */
    public void testUpdatePathMissing() throws Exception {
        Map<String, String> propsSubDir = properties("subDirOwner", "subDirGroup", "0555");
        Map<String, String> propsFile = properties("fileOwner", "fileGroup", "0666");

        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(ggfs, null, null);

        // Set different properties to the sub-directory.
        ggfsSecondary.update(SUBDIR, propsSubDir);

        ggfs.update(FILE, propsFile);

        // Ensure missing entries were re-created locally.
        checkExist(ggfs, DIR, SUBDIR, FILE);

        // Ensure properties propagation.
        assertEquals(propsSubDir, ggfsSecondary.info(SUBDIR).properties());
        assertEquals(propsSubDir, ggfs.info(SUBDIR).properties());

        assertEquals(propsFile, ggfsSecondary.info(FILE).properties());
        assertEquals(propsFile, ggfs.info(FILE).properties());
    }

    /**
     * Test update when parent is the root and the path being updated is missing locally.
     *
     * @throws Exception If failed.
     */
    public void testUpdateParentRootPathMissing() throws Exception {
        Map<String, String> props = properties("owner", "group", "0555");

        create(ggfsSecondary, paths(DIR), null);
        create(ggfs, null, null);

        ggfs.update(DIR, props);

        checkExist(ggfs, DIR);

        assertEquals(props, ggfsSecondary.info(DIR).properties());
        assertEquals(props, ggfs.info(DIR).properties());
    }

    /**
     * Test file open in case it doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    public void testOpenMissing() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, null, null);

        createFile(ggfsSecondary, FILE, true, chunk);

        checkFileContent(ggfs, FILE, chunk);
    }

    /**
     * Ensure that no prefetch occurs in case not enough block are read sequentially.
     *
     * @throws Exception If failed.
     */
    public void testOpenNoPrefetch() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));

        // Write enough data to the secondary file system.
        final int blockSize = GGFS_BLOCK_SIZE;

        IgniteFsOutputStream out = ggfsSecondary.append(FILE, false);

        int totalWritten = 0;

        while (totalWritten < blockSize * 2 + chunk.length) {
            out.write(chunk);

            totalWritten += chunk.length;
        }

        out.close();

        awaitFileClose(ggfsSecondary, FILE);

        // Read the first block.
        int totalRead = 0;

        IgniteFsInputStream in = ggfs.open(FILE, blockSize);

        final byte[] readBuf = new byte[1024];

        while (totalRead + readBuf.length <= blockSize) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        // Now perform seek.
        in.seek(blockSize * 2);

        // Read the third block.
        totalRead = 0;

        while (totalRead < totalWritten - blockSize * 2) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        // Let's wait for a while because prefetch occurs asynchronously.
        U.sleep(300);

        // Remove the file from the secondary file system.
        ggfsSecondary.delete(FILE, false);

        // Let's wait for file will be deleted.
        U.sleep(300);

        final IgniteFsInputStream in0 = in;

        // Try reading the second block. Should fail.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                in0.seek(blockSize);

                try {
                    in0.read(readBuf);
                } finally {
                    U.closeQuiet(in0);
                }

                return null;
            }
        }, IOException.class, "Failed to read data due to secondary file system exception: " +
            "Failed to retrieve file's data block (corrupted file?) [path=/dir/subdir/file, blockIdx=1");
    }

    /**
     * Ensure that prefetch occurs in case several blocks are read sequentially.
     *
     * @throws Exception If failed.
     */
    public void testOpenPrefetch() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));

        // Write enough data to the secondary file system.
        final int blockSize = ggfs.info(FILE).blockSize();

        IgniteFsOutputStream out = ggfsSecondary.append(FILE, false);

        int totalWritten = 0;

        while (totalWritten < blockSize * 2 + chunk.length) {
            out.write(chunk);

            totalWritten += chunk.length;
        }

        out.close();

        awaitFileClose(ggfsSecondary, FILE);

        // Read the first two blocks.
        int totalRead = 0;

        IgniteFsInputStream in = ggfs.open(FILE, blockSize);

        final byte[] readBuf = new byte[1024];

        while (totalRead + readBuf.length <= blockSize * 2) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        // Wait for a while for prefetch to finish.
        GridGgfsMetaManager meta = ggfs.context().meta();

        GridGgfsFileInfo info = meta.info(meta.fileId(FILE));

        GridGgfsBlockKey key = new GridGgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(), 2);

        GridCache<GridGgfsBlockKey, byte[]> dataCache = ggfs.context().kernalContext().cache().cache(
            ggfs.configuration().getDataCacheName());

        for (int i = 0; i < 10; i++) {
            if (dataCache.containsKey(key))
                break;
            else
                U.sleep(100);
        }

        // Remove the file from the secondary file system.
        ggfsSecondary.delete(FILE, false);

        // Let's wait for file will be deleted.
        U.sleep(300);

        // Read the third block.
        totalRead = 0;

        in.seek(blockSize * 2);

        while (totalRead + readBuf.length <= blockSize) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        in.close();
    }

    /**
     * Test create when parent directory is partially missing locally.
     *
     * @throws Exception If failed.
     */
    public void testCreateParentMissingPartially() throws Exception {
        Map<String, String> props = properties("owner", "group", "0555");

        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, paths(DIR), null);

        ggfsSecondary.update(SUBDIR, props);

        createFile(ggfs, FILE, true, chunk);

        // Ensure that directory structure was created.
        checkExist(ggfs, ggfsSecondary, SUBDIR);
        checkFile(ggfs, ggfsSecondary, FILE, chunk);

        // Ensure properties propagation of the created subdirectory.
        assertEquals(props, ggfs.info(SUBDIR).properties());
    }

    /**
     * Test create when parent directory is missing locally.
     *
     * @throws Exception If failed.
     */
    public void testCreateParentMissing() throws Exception {
        Map<String, String> propsDir = properties("ownerDir", "groupDir", "0555");
        Map<String, String> propsSubDir = properties("ownerSubDir", "groupSubDir", "0666");

        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, null, null);

        ggfsSecondary.update(DIR, propsDir);
        ggfsSecondary.update(SUBDIR, propsSubDir);

        createFile(ggfs, FILE, true, chunk);

        checkExist(ggfs, ggfsSecondary, SUBDIR);
        checkFile(ggfs, ggfsSecondary, FILE, chunk);

        // Ensure properties propagation of the created directories.
        assertEquals(propsDir, ggfs.info(DIR).properties());
        assertEquals(propsSubDir, ggfs.info(SUBDIR).properties());
    }

    /**
     * Test append when parent directory is partially missing locally.
     *
     * @throws Exception If failed.
     */
    public void testAppendParentMissingPartially() throws Exception {
        Map<String, String> props = properties("owner", "group", "0555");

        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, paths(DIR), null);

        ggfsSecondary.update(SUBDIR, props);

        createFile(ggfsSecondary, FILE, true, BLOCK_SIZE, chunk);

        appendFile(ggfs, FILE, chunk);

        // Ensure that directory structure was created.
        checkExist(ggfs, ggfsSecondary, SUBDIR);
        checkFile(ggfs, ggfsSecondary, FILE, chunk, chunk);

        // Ensure properties propagation of the created subdirectory.
        assertEquals(props, ggfs.info(SUBDIR).properties());
    }

    /**
     * Test append when parent directory is missing locally.
     *
     * @throws Exception If failed.
     */
    public void testAppendParentMissing() throws Exception {
        Map<String, String> propsDir = properties("ownerDir", "groupDir", "0555");
        Map<String, String> propsSubDir = properties("ownerSubDir", "groupSubDir", "0666");

        create(ggfsSecondary, paths(DIR, SUBDIR), null);
        create(ggfs, null, null);

        ggfsSecondary.update(DIR, propsDir);
        ggfsSecondary.update(SUBDIR, propsSubDir);

        createFile(ggfsSecondary, FILE, true, BLOCK_SIZE, chunk);

        appendFile(ggfs, FILE, chunk);

        checkExist(ggfs, ggfsSecondary, SUBDIR);
        checkFile(ggfs, ggfsSecondary, FILE, chunk, chunk);

        // Ensure properties propagation of the created directories.
        assertEquals(propsDir, ggfs.info(DIR).properties());
        assertEquals(propsSubDir, ggfs.info(SUBDIR).properties());
    }

    /**
     * Ensure that in case we rename the folder A and delete it at the same time, only one of these requests succeed.
     * Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenameDeleteSourceRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW), paths());

            GridPlainFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        ggfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteCheckedException ignored) {
                        return false;
                    }
                }
            });

            GridPlainFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    return ggfs.delete(SUBDIR, true);
                }
            });

            res1.get();
            res2.get();

            if (res1.get()) {
                assert !res2.get(); // Rename succeeded, so delete must fail.

                checkExist(ggfs, ggfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(ggfs, ggfsSecondary, SUBDIR);
            }
            else {
                assert res2.get(); // Rename failed because delete succeeded.

                checkExist(ggfs, DIR); // DIR_NEW should not be synchronized with he primary GGFS.
                checkExist(ggfsSecondary, DIR, DIR_NEW);
                checkNotExist(ggfs, ggfsSecondary, SUBDIR, SUBDIR_NEW);
            }

            clear(ggfs, ggfsSecondary);
        }
    }

    /**
     * Ensure that in case we rename the folder A to B and delete B at the same time, FS consistency is not compromised.
     * Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenameDeleteDestinationRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW), paths());

            GridPlainFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        ggfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteCheckedException ignored) {
                        return false;
                    }
                }
            });

            GridPlainFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    return ggfs.delete(SUBDIR_NEW, true);
                }
            });

            assert res1.get();

            if (res2.get()) {
                // Delete after rename.
                checkExist(ggfs, ggfsSecondary, DIR, DIR_NEW);
                checkNotExist(ggfs, ggfsSecondary, SUBDIR, SUBDIR_NEW);
            }
            else {
                // Delete before rename.
                checkExist(ggfs, ggfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(ggfs, ggfsSecondary, SUBDIR);
            }

            clear(ggfs, ggfsSecondary);
        }
    }

    /**
     * Ensure file system consistency in case two concurrent rename requests are executed: A -> B and B -> A.
     * Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenamesRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(ggfsSecondary, paths(DIR, SUBDIR, DIR_NEW), paths());

            GridPlainFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        ggfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteCheckedException ignored) {
                        return false;
                    }
                }
            });

            GridPlainFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        ggfs.rename(SUBDIR_NEW, SUBDIR);

                        return true;
                    }
                    catch (IgniteCheckedException ignored) {
                        return false;
                    }
                }
            });

            res1.get();
            res2.get();

            assert res1.get(); // First rename must be successful anyway.

            if (res2.get()) {
                checkExist(ggfs, ggfsSecondary, DIR, SUBDIR, DIR_NEW);
                checkNotExist(ggfs, ggfsSecondary, SUBDIR_NEW);
            }
            else {
                checkExist(ggfs, ggfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(ggfs, ggfsSecondary, SUBDIR);
            }

            clear(ggfs, ggfsSecondary);
        }
    }

    /**
     * Ensure that in case we delete the folder A and delete its parent at the same time, resulting file system
     * structure is consistent. Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentDeletesRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(ggfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths());

            GridPlainFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        ggfs.delete(SUBDIR, true);

                        return true;
                    }
                    catch (IgniteCheckedException ignored) {
                        return false;
                    }
                }
            });

            GridPlainFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        ggfs.delete(SUBSUBDIR, true);

                        return true;
                    }
                    catch (IgniteCheckedException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get(); // Delete on the parent must succeed anyway.
            res2.get();

            checkExist(ggfs, ggfsSecondary, DIR);
            checkNotExist(ggfs, ggfsSecondary, SUBDIR, SUBSUBDIR);

            clear(ggfs, ggfsSecondary);
        }
    }
}
