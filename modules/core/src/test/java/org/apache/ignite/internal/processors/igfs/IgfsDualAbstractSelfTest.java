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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for IGFS working in mode when remote file system exists: DUAL_SYNC, DUAL_ASYNC.
 */
@SuppressWarnings("ConstantConditions")
@RunWith(JUnit4.class)
public abstract class IgfsDualAbstractSelfTest extends IgfsAbstractSelfTest {
    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     */
    protected IgfsDualAbstractSelfTest(IgfsMode mode) {
        super(mode);
    }

    /**
     * Test existence check when the path exists only remotely.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExistsPathMissing() throws Exception {
        create(igfsSecondary, paths(DIR), null);

        assert igfs.exists(DIR);
    }

    /**
     * Test list files routine when the path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testListFilesPathMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        Collection<IgfsFile> paths = igfs.listFiles(SUBDIR);

        assert paths != null;
        assert paths.size() == 2;

        Iterator<IgfsFile> iter = paths.iterator();

        IgfsFile path1 = iter.next();
        IgfsFile path2 = iter.next();

        assert (SUBSUBDIR.equals(path1.path()) && FILE.equals(path2.path())) ||
            (FILE.equals(path1.path()) && SUBSUBDIR.equals(path2.path()));
    }

    /**
     * Test info routine when the path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInfoPathMissing() throws Exception {
        create(igfsSecondary, paths(DIR), null);
        create(igfs, null, null);

        IgfsFile info = igfs.info(DIR);

        assert info != null;

        assertEquals(DIR, info.path());
    }

    /**
     * Test rename in case source exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameFileSourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, paths(DIR), null);

        igfs.rename(FILE, FILE2);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, FILE2);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test rename in case source doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameFileSourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, null, null);

        igfs.rename(FILE, FILE2);

        checkExist(igfs, DIR, SUBDIR);
        checkExist(igfs, igfsSecondary, FILE2);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file rename when parent folder is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameFileParentRootSourceMissing() throws Exception {
        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        create(igfsSecondary, null, paths(file1));
        create(igfs, null, null);

        igfs.rename(file1, file2);

        checkExist(igfs, igfsSecondary, file2);
        checkNotExist(igfs, igfsSecondary, file1);
    }

    /**
     * Test rename in case source exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameDirectorySourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, paths(DIR), null);

        igfs.rename(SUBDIR, SUBDIR2);

        checkExist(igfs, igfsSecondary, SUBDIR2);
        checkNotExist(igfs, igfsSecondary, SUBDIR);
    }

    /**
     * Test rename in case source doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameDirectorySourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, null, null);

        igfs.rename(SUBDIR, SUBDIR2);

        checkExist(igfs, DIR);
        checkExist(igfs, igfsSecondary, SUBDIR2);
        checkNotExist(igfs, igfsSecondary, SUBDIR);
    }

    /**
     * Test directory rename when parent folder is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameDirectoryParentRootSourceMissing() throws Exception {
        IgfsPath dir1 = new IgfsPath("/dir1");
        IgfsPath dir2 = new IgfsPath("/dir2");

        create(igfsSecondary, paths(dir1), null);
        create(igfs, null, null);

        igfs.rename(dir1, dir2);

        checkExist(igfs, igfsSecondary, dir2);
        checkNotExist(igfs, igfsSecondary, dir1);
    }

    /**
     * Test move in case source exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileSourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move in case source doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileSourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, DIR, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move in case destination exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths(FILE));

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move in case destination doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move in case source and destination exist partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileSourceAndDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, DIR_NEW), null);

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, SUBDIR, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move in case source and destination don't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileSourceAndDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, null, null);

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileDestinationRootSourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, paths(DIR), null);

        igfs.rename(FILE, IgfsPath.ROOT);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath("/" + FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileDestinationRootSourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, null, null);

        igfs.rename(FILE, IgfsPath.ROOT);

        checkExist(igfs, DIR, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath("/" + FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move when source parent is the root and the source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileSourceParentRootSourceMissing() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(file, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test file move when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileSourceParentRootDestinationMissingPartially() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(igfs, paths(DIR_NEW), null);

        igfs.rename(file, SUBDIR_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test file move when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveFileSourceParentRootDestinationMissing() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(igfs, null, null);

        igfs.rename(file, SUBDIR_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test move and rename in case source exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileSourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move and rename in case source doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileSourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, DIR, SUBDIR);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move and rename in case destination exists partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths(FILE));

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move and rename in case destination doesn't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move and rename in case source and destination exist partially and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileSourceAndDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, paths(DIR, DIR_NEW), null);

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, SUBDIR, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test move and rename in case source and destination don't exist and the path being renamed is a file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileSourceAndDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));
        create(igfs, null, null);

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move and rename when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileDestinationRootSourceMissingPartially() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, paths(DIR), null);

        igfs.rename(FILE, file);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, file);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move and rename when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileDestinationRootSourceMissing() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, null, null);

        igfs.rename(FILE, file);

        checkExist(igfs, DIR, SUBDIR);
        checkExist(igfs, igfsSecondary, file);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move and rename when source parent is the root and the source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileSourceParentRootSourceMissing() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE_NEW.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(file, FILE_NEW);

        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test file move and rename when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileSourceParentRootDestinationMissingPartially() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE_NEW.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(igfs, paths(DIR_NEW), null);

        igfs.rename(file, FILE_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test file move and rename when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameFileSourceParentRootDestinationMissing() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE_NEW.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW), paths(file));
        create(igfs, null, null);

        igfs.rename(file, FILE_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test move in case source exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectorySourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case source doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectorySourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, DIR);
        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case destination exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectoryDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case destination doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectoryDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case source and destination exist partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectorySourceAndDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, DIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, SUBDIR, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move in case source and destination don't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectorySourceAndDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, null, null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory  move when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectoryDestinationRootSourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(igfs, paths(DIR), null);

        igfs.rename(SUBSUBDIR, IgfsPath.ROOT);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath("/" + SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectoryDestinationRootSourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(igfs, null, null);

        igfs.rename(SUBSUBDIR, IgfsPath.ROOT);

        checkExist(igfs, DIR, SUBDIR);
        checkExist(igfs, igfsSecondary, new IgfsPath("/" + SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when source parent is the root and the source folder is missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectorySourceParentRootSourceMissing() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(dir, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test directory move when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectorySourceParentRootDestinationMissingPartially() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(igfs, paths(DIR_NEW), null);

        igfs.rename(dir, SUBDIR_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test directory move when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveDirectorySourceParentRootDestinationMissing() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(igfs, null, null);

        igfs.rename(dir, SUBDIR_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test move and rename in case source exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectorySourceMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case source doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectorySourceMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, DIR);
        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case destination exists partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectoryDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case destination doesn't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectoryDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case source and destination exist partially and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectorySourceAndDestinationMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, paths(DIR, DIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, SUBDIR, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test move and rename in case source and destination don't exist and the path being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectorySourceAndDestinationMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);
        create(igfs, null, null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, DIR, SUBDIR, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when destination is the root and source is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectoryDestinationRootSourceMissingPartially() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(igfs, paths(DIR), null);

        igfs.rename(SUBSUBDIR, dir);

        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, dir);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when destination is the root and source is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectoryDestinationRootSourceMissing() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), null);
        create(igfs, null, null);

        igfs.rename(SUBSUBDIR, dir);

        checkExist(igfs, DIR, SUBDIR);
        checkExist(igfs, igfsSecondary, dir);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when source parent is the root and the source is missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectorySourceParentRootSourceMissing() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR_NEW.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(igfs, paths(DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test directory move and rename when source parent is the root and destination is missing partially.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectorySourceParentRootDestinationMissingPartially() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR_NEW.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(igfs, paths(DIR_NEW), null);

        igfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(igfs, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test directory move and rename when source parent is the root and destination is missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMoveRenameDirectorySourceParentRootDestinationMissing() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR_NEW.name());

        create(igfsSecondary, paths(DIR_NEW, SUBDIR_NEW, dir), null);
        create(igfs, null, null);

        igfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(igfs, DIR_NEW, SUBDIR_NEW);
        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test mkdirs in case parent exists remotely, but some part of the parent path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMkdirsParentPathMissingPartially() throws Exception {
        Map<String, String> props = null;

        if (permissionsSupported())
            props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, paths(DIR), null);

        igfs.mkdirs(SUBSUBDIR, props);

        // Ensure that directory was created and properties are propagated.
        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, SUBSUBDIR);

        if (permissionsSupported()) {
            // Check only permissions because user and group will always be present in Hadoop secondary filesystem.
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION), igfsSecondary.permissions(SUBSUBDIR.toString()));

            // We check only permission because IGFS client adds username and group name explicitly.
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION),
                igfs.info(SUBSUBDIR).properties().get(IgfsUtils.PROP_PERMISSION));
        }
    }

    /**
     * Test mkdirs in case parent exists remotely, but no parents exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMkdrisParentPathMissing() throws Exception {
        Map<String, String> props = null;

        if (permissionsSupported())
            props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, null, null);

        igfs.mkdirs(SUBSUBDIR, props);

        // Ensure that directory was created and properties are propagated.
        checkExist(igfs, DIR);
        checkExist(igfs, SUBDIR);
        checkExist(igfs, igfsSecondary, SUBSUBDIR);

        if (permissionsSupported()) {
            // Check only permission because in case of Hadoop secondary Fs user and group will always be present:
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION), igfsSecondary.permissions(SUBSUBDIR.toString()));

            // We check only permission because IGFS client adds username and group name explicitly.
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION),
                igfs.info(SUBSUBDIR).properties().get(IgfsUtils.PROP_PERMISSION));
        }
    }

    /**
     * Test delete in case parent exists remotely, but some part of the parent path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeletePathMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        create(igfs, paths(DIR), null);

        igfs.delete(SUBDIR, true);

        checkExist(igfs, DIR);
        checkNotExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test delete in case parent exists remotely, but no parents exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeletePathMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        create(igfs, null, null);

        igfs.delete(SUBDIR, true);

        checkExist(igfs, DIR);
        checkNotExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test delete when the path parent is the root and the path is missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeleteParentRootPathMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        create(igfs, null, null);

        igfs.delete(DIR, true);

        checkNotExist(igfs, igfsSecondary, DIR, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test update in case file exists remotely, but some part of the path doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatePathMissingPartially() throws Exception {
        if(!propertiesSupported())
            return;

        Map<String, String> propsSubDir = properties("subDirOwner", "subDirGroup", "0555");
        Map<String, String> propsFile = properties("fileOwner", "fileGroup", "0666");

        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, paths(DIR), null);

        // Set different properties to the sub-directory.
        igfsSecondaryFileSystem.update(SUBDIR, propsSubDir);

        igfs.update(FILE, propsFile);

        // Ensure missing entries were re-created locally.
        checkExist(igfs, SUBDIR, FILE);

        // Ensure properties propagation.
        assertEquals(propsSubDir, igfsSecondary.properties(SUBDIR.toString()));
        assertEquals(propsSubDir, igfs.info(SUBDIR).properties());

        assertEquals(propsFile, igfsSecondary.properties(FILE.toString()));
        assertEquals(propsFile, igfs.info(FILE).properties());
    }

    /**
     * Test update in case file exists remotely, but neither the file nor all it's parents exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatePathMissing() throws Exception {
        if(!propertiesSupported())
            return;

        Map<String, String> propsSubDir = properties("subDirOwner", "subDirGroup", "0555");
        Map<String, String> propsFile = properties("fileOwner", "fileGroup", "0666");

        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));
        create(igfs, null, null);

        // Set different properties to the sub-directory.
        igfsSecondaryFileSystem.update(SUBDIR, propsSubDir);

        igfs.update(FILE, propsFile);

        // Ensure missing entries were re-created locally.
        checkExist(igfs, DIR, SUBDIR, FILE);

        // Ensure properties propagation.
        assertEquals(propsSubDir, igfsSecondary.properties(SUBDIR.toString()));
        assertEquals(propsSubDir, igfs.info(SUBDIR).properties());

        assertEquals(propsFile, igfsSecondary.properties(FILE.toString()));
        assertEquals(propsFile, igfs.info(FILE).properties());
    }

    /**
     * Test update when parent is the root and the path being updated is missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateParentRootPathMissing() throws Exception {
        doUpdateParentRootPathMissing(properties("owner", "group", "0555"));
    }

    /**
     * Test update when parent is the root and the path being updated is missing locally.
     *
     * @param props Properties.
     * @throws Exception If failed.
     */
    protected void doUpdateParentRootPathMissing(Map<String, String> props) throws Exception {
        if (!propertiesSupported())
            return;

        create(igfsSecondary, paths(DIR), null);
        create(igfs, null, null);

        igfs.update(DIR, props);

        checkExist(igfs, DIR);

        assertTrue(propertiesContains(igfsSecondary.properties(DIR.toString()), props));
        assertTrue(propertiesContains(igfs.info(DIR).properties(), props));
    }

    /**
     * Test file open in case it doesn't exist locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOpenMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, null, null);

        createFile(igfsSecondary, FILE, chunk);

        checkFileContent(igfs, FILE, chunk);
    }

    /**
     * Test create when parent directory is partially missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateParentMissingPartially() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, paths(DIR), null);

        createFile(igfs, FILE, true, chunk);

        // Ensure that directory structure was created.
        checkExist(igfs, igfsSecondary, SUBDIR);
        checkFile(igfs, igfsSecondary, FILE, chunk);
    }

    /**
     * Test properties set on partially missing directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetPropertiesOnPartiallyMissingDirectory() throws Exception {
        if (!propertiesSupported())
            return;

        Map<String, String> props = properties("owner", "group", "0555");

        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, paths(DIR), null);

        igfsSecondaryFileSystem.update(SUBDIR, props);

        // Ensure properties propagation of the created subdirectory.
        assertEquals(props, igfs.info(SUBDIR).properties());
    }

    /**
     * Test create when parent directory is missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateParentMissing() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, null, null);

        createFile(igfs, FILE, true, chunk);

        checkExist(igfs, igfsSecondary, SUBDIR);
        checkFile(igfs, igfsSecondary, FILE, chunk);
    }

    /**
     * Test properties set on missing directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetPropertiesOnMissingDirectory() throws Exception {
        if (!propertiesSupported())
            return;

        Map<String, String> propsDir = properties("ownerDir", "groupDir", "0555");
        Map<String, String> propsSubDir = properties("ownerSubDir", "groupSubDir", "0666");

        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, null, null);

        igfsSecondaryFileSystem.update(DIR, propsDir);
        igfsSecondaryFileSystem.update(SUBDIR, propsSubDir);

        // Ensure properties propagation of the created directories.
        assertEquals(propsDir, igfs.info(DIR).properties());
        assertEquals(propsSubDir, igfs.info(SUBDIR).properties());
    }

    /**
     * Test append when parent directory is partially missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAppendParentMissingPartially() throws Exception {
        if (!appendSupported())
            return;

        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, paths(DIR), null);

        createFile(igfsSecondary, FILE, /*BLOCK_SIZE,*/ chunk);

        appendFile(igfs, FILE, chunk);

        // Ensure that directory structure was created.
        checkExist(igfs, igfsSecondary, SUBDIR);
        checkFile(igfs, igfsSecondary, FILE, chunk, chunk);
    }

    /**
     * Test append when parent directory is missing locally.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAppendParentMissing() throws Exception {
        if (!appendSupported())
            return;

        create(igfsSecondary, paths(DIR, SUBDIR), null);
        create(igfs, null, null);

        createFile(igfsSecondary, FILE, /*BLOCK_SIZE,*/ chunk);

        appendFile(igfs, FILE, chunk);

        checkExist(igfs, igfsSecondary, SUBDIR);
        checkFile(igfs, igfsSecondary, FILE, chunk, chunk);
    }

    /**
     * Ensure that in case we rename the folder A and delete it at the same time, only one of these requests succeed.
     * Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentRenameDeleteSourceRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    return igfs.delete(SUBDIR, true);
                }
            });

            res1.get();
            res2.get();

            if (res1.get()) {
                assert !res2.get(); // Rename succeeded, so delete must fail.

                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }
            else {
                assert res2.get(); // Rename failed because delete succeeded.

                checkExist(igfs, DIR); // DIR_NEW should not be synchronized with he primary IGFS.
                checkExist(igfsSecondary, DIR, DIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we rename the folder A to B and delete B at the same time, FS consistency is not compromised.
     * Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentRenameDeleteDestinationRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    return igfs.delete(SUBDIR_NEW, true);
                }
            });

            assert res1.get();

            if (res2.get()) {
                // Delete after rename.
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
            }
            else {
                // Delete before rename.
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure file system consistency in case two concurrent rename requests are executed: A -> B and B -> A.
     * Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentRenamesRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR_NEW, SUBDIR);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            res1.get();
            res2.get();

            assert res1.get(); // First rename must be successful anyway.

            if (res2.get()) {
                checkExist(igfs, igfsSecondary, DIR, SUBDIR, DIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR_NEW);
            }
            else {
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we delete the folder A and delete its parent at the same time, resulting file system
     * structure is consistent. Initially file system entries are created only in the secondary file system.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentDeletesRemote() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfsSecondary, paths(DIR, SUBDIR, SUBSUBDIR), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.delete(SUBDIR, true);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.delete(SUBSUBDIR, true);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get(); // Delete on the parent must succeed anyway.
            res2.get();

            checkExist(igfs, igfsSecondary, DIR);
            checkNotExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR);

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Checks file access & modification time equality in the file itself and in the same file found through
     * the listing of its parent.
     *
     * @param fs The file system.
     * @param p The file path.
     *
     * @return Tuple of access and modification times of the file.
     */
    private T2<Long, Long> checkParentListingTime(IgfsSecondaryFileSystem fs, IgfsPath p) {
        IgfsFile f0 = fs.info(p);

        T2<Long, Long> t0 = new T2<>(f0.accessTime(), f0.modificationTime());

        // Root cannot be seen through the parent listing:
        if (!F.eq(IgfsPath.ROOT, p)) {
            assertNotNull(f0);

            Collection<IgfsFile> listing = fs.listFiles(p.parent());

            IgfsFile f1 = null;

            for (IgfsFile fi : listing) {
                if (F.eq(fi.path(), p)) {
                    f1 = fi;

                    break;
                }
            }

            assertNotNull(f1); // file should be found in parent listing.

            T2<Long, Long> t1 = new T2<>(f1.accessTime(), f1.modificationTime());

            assertEquals(t0, t1);
        }

        return t0;
    }

    /**
     * Test for file modification time upwards propagation when files are
     * created on the secondary file system and initially
     * unknown on the primary file system.
     *
     * @throws Exception On error.
     */
    @Test
    public void testAccessAndModificationTimeUpwardsPropagation() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE, FILE2));

        T2<Long,Long> timesDir0 = checkParentListingTime(igfsSecondaryFileSystem, DIR);
        T2<Long,Long> timesSubDir0 = checkParentListingTime(igfsSecondaryFileSystem, SUBDIR);
        T2<Long,Long> timesFile0 = checkParentListingTime(igfsSecondaryFileSystem, FILE);
        T2<Long,Long> timesFile20 = checkParentListingTime(igfsSecondaryFileSystem, FILE2);

        Thread.sleep(500L);

        T2<Long,Long> timesDir1 = checkParentListingTime(igfs.asSecondary(), DIR);
        T2<Long,Long> timesSubDir1 = checkParentListingTime(igfs.asSecondary(), SUBDIR);
        T2<Long,Long> timesFile1 = checkParentListingTime(igfs.asSecondary(), FILE);
        T2<Long,Long> timesFile21 = checkParentListingTime(igfs.asSecondary(), FILE2);

        assertEquals(timesDir0, timesDir1);
        assertEquals(timesSubDir0, timesSubDir1);
        assertEquals(timesFile0, timesFile1);
        assertEquals(timesFile20, timesFile21);
    }

    /**
     * Test setTimes method when path is partially missing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetTimesMissingPartially() throws Exception {
        if (!timesSupported())
            return;

        create(igfs, paths(DIR, SUBDIR), null);

        createFile(igfsSecondary, FILE, chunk);

        final long MAX_ALIGN_ON_SECOND = (long)Integer.MAX_VALUE * 1000;

        igfs.setTimes(FILE, MAX_ALIGN_ON_SECOND, MAX_ALIGN_ON_SECOND - 1000);

        IgfsFile info = igfs.info(FILE);

        assert info != null;

        assertEquals(MAX_ALIGN_ON_SECOND - 1000, info.accessTime());
        assertEquals(MAX_ALIGN_ON_SECOND, info.modificationTime());

        T2<Long, Long> secondaryTimes = igfsSecondary.times(FILE.toString());

        assertEquals(info.modificationTime(), (long) secondaryTimes.get1());
        assertEquals(info.accessTime(), (long) secondaryTimes.get2());

        try {
            igfs.setTimes(FILE2, MAX_ALIGN_ON_SECOND, MAX_ALIGN_ON_SECOND);

            fail("Exception is not thrown for missing file.");
        } catch (Exception ignore) {
            // No-op.
        }
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSecondarySize() throws Exception {
        igfs.mkdirs(SUBDIR);

        createFile(igfsSecondary, FILE, chunk);
        createFile(igfsSecondary, new IgfsPath(SUBDIR, "file2"), chunk);

        assertEquals(chunk.length, igfs.size(FILE));
        assertEquals(chunk.length * 2, igfs.size(SUBDIR));
    }

    /**
     * @param allProps All properties.
     * @param checkedProps Checked properies
     * @return {@code true} If allchecked properties are contained in the #propsAll.
     */
    public static boolean propertiesContains(Map<String, String> allProps, Map<String, String> checkedProps) {
        for (String name : checkedProps.keySet())
            if (!checkedProps.get(name).equals(allProps.get(name))) {
                System.err.println("All properties: " + allProps);
                System.err.println("Checked properties: " + checkedProps);
                return false;
            }

        return true;
    }
}
