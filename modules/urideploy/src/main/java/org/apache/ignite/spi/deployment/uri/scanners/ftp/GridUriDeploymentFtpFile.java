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

package org.apache.ignite.spi.deployment.uri.scanners.ftp;

import com.enterprisedt.net.ftp.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import java.util.*;

/**
 * Value object which encapsulates {@link FTPFile} and corresponding directory.
 */
class GridUriDeploymentFtpFile {
    /** */
    private final String dir;

    /** */
    private final FTPFile file;

    /**
     * @param dir Remote FTP directory.
     * @param file FTP file.
     */
    GridUriDeploymentFtpFile(String dir, FTPFile file) {
        assert dir != null;
        assert file != null;
        assert file.getName() != null;

        this.dir = dir;
        this.file = file;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof GridUriDeploymentFtpFile))
            return false;

        GridUriDeploymentFtpFile other = (GridUriDeploymentFtpFile)obj;

        return dir.equals(other.dir) && file.getName().equals(other.file.getName());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = dir.hashCode();

        res = 29 * res + file.getName().hashCode();

        return res;
    }

    /**
     * @return File name.
     */
    String getName() {
        return file.getName();
    }

    /**
     * @return Last modification date as calendar.
     */
    Calendar getTimestamp() {
        Date date = file.lastModified();

        Calendar cal = null;

        if (date != null) {
            cal = Calendar.getInstance();

            cal.setTime(date);
        }

        return cal;
    }

    /**
     * @return {@code True} in case provided file is a directory.
     */
    boolean isDirectory() {
        return file.isDir();
    }

    /**
     * @return {@code True} in case provided file is not a directory or link.
     */
    boolean isFile() {
        return !file.isDir() && !file.isLink();
    }

    /**
     * @return File parent directory.
     */
    String getParentDirectory() {
        return dir;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentFtpFile.class, this);
    }
}
