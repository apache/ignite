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
package jakarta.fileupload;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.util.List;

/**
 * <p>High level API for processing file uploads.</p>
 *
 * <p>This class handles multiple files per single HTML widget, sent using
 * <code>multipart/mixed</code> encoding type, as specified by
 * <a href="http://www.ietf.org/rfc/rfc1867.txt">RFC 1867</a>.  Use {@link
 * #parseRequest(HttpServletRequest)} to acquire a list of {@link
 * jakarta.fileupload.FileItem}s associated with a given HTML
 * widget.</p>
 *
 * <p>Individual parts will be stored in temporary disk storage or in memory,
 * depending on their size, and will be available as {@link
 * jakarta.fileupload.FileItem}s.</p>
 *
 * @deprecated 1.1 Use <code>ServletFileUpload</code> together with
 *             <code>DiskFileItemFactory</code> instead.
 */
@Deprecated
public class DiskFileUpload
    extends FileUploadBase {

    // ----------------------------------------------------------- Data members

    /**
     * The factory to use to create new form items.
     */
    private DefaultFileItemFactory fileItemFactory;

    // ----------------------------------------------------------- Constructors

    /**
     * Constructs an instance of this class which uses the default factory to
     * create <code>FileItem</code> instances.
     *
     * @see #DiskFileUpload(DefaultFileItemFactory fileItemFactory)
     *
     * @deprecated 1.1 Use <code>FileUpload</code> instead.
     */
    @Deprecated
    public DiskFileUpload() {
        super();
        this.fileItemFactory = new DefaultFileItemFactory();
    }

    /**
     * Constructs an instance of this class which uses the supplied factory to
     * create <code>FileItem</code> instances.
     *
     * @see #DiskFileUpload()
     * @param fileItemFactory The file item factory to use.
     *
     * @deprecated 1.1 Use <code>FileUpload</code> instead.
     */
    @Deprecated
    public DiskFileUpload(DefaultFileItemFactory fileItemFactory) {
        super();
        this.fileItemFactory = fileItemFactory;
    }

    // ----------------------------------------------------- Property accessors

    /**
     * Returns the factory class used when creating file items.
     *
     * @return The factory class for new file items.
     *
     * @deprecated 1.1 Use <code>FileUpload</code> instead.
     */
    @Override
    @Deprecated
    public FileItemFactory getFileItemFactory() {
        return fileItemFactory;
    }

    /**
     * Sets the factory class to use when creating file items. The factory must
     * be an instance of <code>DefaultFileItemFactory</code> or a subclass
     * thereof, or else a <code>ClassCastException</code> will be thrown.
     *
     * @param factory The factory class for new file items.
     *
     * @deprecated 1.1 Use <code>FileUpload</code> instead.
     */
    @Override
    @Deprecated
    public void setFileItemFactory(FileItemFactory factory) {
        this.fileItemFactory = (DefaultFileItemFactory) factory;
    }

    /**
     * Returns the size threshold beyond which files are written directly to
     * disk.
     *
     * @return The size threshold, in bytes.
     *
     * @see #setSizeThreshold(int)
     *
     * @deprecated 1.1 Use <code>DiskFileItemFactory</code> instead.
     */
    @Deprecated
    public int getSizeThreshold() {
        return fileItemFactory.getSizeThreshold();
    }

    /**
     * Sets the size threshold beyond which files are written directly to disk.
     *
     * @param sizeThreshold The size threshold, in bytes.
     *
     * @see #getSizeThreshold()
     *
     * @deprecated 1.1 Use <code>DiskFileItemFactory</code> instead.
     */
    @Deprecated
    public void setSizeThreshold(int sizeThreshold) {
        fileItemFactory.setSizeThreshold(sizeThreshold);
    }

    /**
     * Returns the location used to temporarily store files that are larger
     * than the configured size threshold.
     *
     * @return The path to the temporary file location.
     *
     * @see #setRepositoryPath(String)
     *
     * @deprecated 1.1 Use <code>DiskFileItemFactory</code> instead.
     */
    @Deprecated
    public String getRepositoryPath() {
        return fileItemFactory.getRepository().getPath();
    }

    /**
     * Sets the location used to temporarily store files that are larger
     * than the configured size threshold.
     *
     * @param repositoryPath The path to the temporary file location.
     *
     * @see #getRepositoryPath()
     *
     * @deprecated 1.1 Use <code>DiskFileItemFactory</code> instead.
     */
    @Deprecated
    public void setRepositoryPath(String repositoryPath) {
        fileItemFactory.setRepository(new File(repositoryPath));
    }

    // --------------------------------------------------------- Public methods

    /**
     * Processes an <a href="http://www.ietf.org/rfc/rfc1867.txt">RFC 1867</a>
     * compliant <code>multipart/form-data</code> stream. If files are stored
     * on disk, the path is given by <code>getRepository()</code>.
     *
     * @param req           The servlet request to be parsed. Must be non-null.
     * @param sizeThreshold The max size in bytes to be stored in memory.
     * @param sizeMax       The maximum allowed upload size, in bytes.
     * @param path          The location where the files should be stored.
     *
     * @return A list of <code>FileItem</code> instances parsed from the
     *         request, in the order that they were transmitted.
     *
     * @throws FileUploadException if there are problems reading/parsing
     *                             the request or storing files.
     *
     * @deprecated 1.1 Use <code>ServletFileUpload</code> instead.
     */
    @Deprecated
    public List<FileItem> parseRequest(HttpServletRequest req,
                                            int sizeThreshold,
                                            long sizeMax, String path)
        throws FileUploadException {
        setSizeThreshold(sizeThreshold);
        setSizeMax(sizeMax);
        setRepositoryPath(path);
        return parseRequest(req);
    }

}
