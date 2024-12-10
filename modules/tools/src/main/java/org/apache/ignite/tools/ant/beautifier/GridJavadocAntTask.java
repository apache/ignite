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

package org.apache.ignite.tools.ant.beautifier;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import jodd.jerry.Jerry;
import jodd.lagarto.dom.LagartoDOMBuilder;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.MatchingTask;

/**
 * Ant task fixing known HTML issues for Javadoc.
 */
public class GridJavadocAntTask extends MatchingTask {
    /** Directory. */
    private File dir;

    /** Whether to verify JavaDoc HTML. */
    private boolean verify = true;

    /**
     * Sets directory.
     *
     * @param dir Directory to set.
     */
    public void setDir(File dir) {
        assert dir != null;

        this.dir = dir;
    }

    /**
     * Sets whether to verify JavaDoc HTML.
     *
     * @param verify Verify flag.
     */
    public void setVerify(Boolean verify) {
        assert verify != null;

        this.verify = verify;
    }

    /**
     * Closes resource.
     *
     * @param closeable Resource to close.
     */
    private void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (IOException e) {
                log("Failed closing [resource=" + closeable + ", message=" + e.getLocalizedMessage() + ']',
                    Project.MSG_WARN);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void execute() {
        if (dir == null)
            throw new BuildException("'dir' attribute must be specified.");

        log("dir=" + dir, Project.MSG_DEBUG);

        DirectoryScanner scanner = getDirectoryScanner(dir);

        boolean fail = false;

        ArrayList<String> errMsgs = new ArrayList<>();

        for (String fileName : scanner.getIncludedFiles()) {
            String file = dir.getAbsolutePath() + '/' + fileName;

            try {
                processFile(file);
            }
            catch (IOException e) {
                throw new BuildException("IO error while processing: " + file, e);
            }
            catch (IllegalArgumentException e) {
                System.err.println("JavaDoc error: " + e.getMessage());

                errMsgs.add(e.getMessage());

                fail = true;
            }
        }

        if (fail)
            throw new BuildException("Execution failed due to: " + prepareErrorSummary(errMsgs));
    }

    /**
     * @param errMsgs Err msgs.
     */
    private String prepareErrorSummary(ArrayList<String> errMsgs) {
        StringBuilder strBdr = new StringBuilder();

        for (String errMsg : errMsgs)
            strBdr.append(errMsg).append(System.lineSeparator());

        return strBdr.toString();
    }

    /**
     * Processes file (validating Javadoc's HTML).
     *
     * @param fileName File to validate.
     * @throws IOException Thrown in case of any I/O error.
     * @throws IllegalArgumentException In JavaDoc HTML validation failed.
     */
    private void processFile(String fileName) throws IOException {
        assert fileName != null;

        File file = new File(fileName);

        String fileContent = readFileToString(file);

        if (verify) {
            // Parse HTML.
            Jerry doc = Jerry.create(
                    new LagartoDOMBuilder()
                            .enableHtmlMode()
                            .configure(cfg -> cfg.setErrorLogEnabled(false))
            ).parse(fileContent);

            if (!"11".equals(System.getProperty("java.specification.version"))) {
                throw new IllegalArgumentException("GridJavadocAntTask isn't tested for java versions after 11. " +
                    "Please check html rendering of documentation package groups works correctly and remove this exception then.");
            }

            if ("index.html".equals(file.getName())) {
                // Try to find Other Packages section.
                Jerry otherPackages =
                    doc.find("div.contentContainer table.overviewSummary caption span:contains('Other Packages')");

                if (otherPackages.size() > 0) {
                    System.err.println("[ERROR]: 'Other Packages' section should not be present, but found: " +
                        doc.html());
                    throw new IllegalArgumentException("'Other Packages' section should not be present, " +
                        "all packages should have corresponding documentation groups: " + file + ";" +
                        "Please add packages description to parent/pom.xml into <plugin>(maven-javadoc-plugin) / " +
                        "<configuration> / <groups>");
                }

                int pkgGrps = doc.find("div.contentContainer table.overviewSummary caption span.tableTab").size();

                if (pkgGrps == 0) {
                    throw new IllegalArgumentException("Documentation package groups missed. Please add packages " +
                        "description to parent/pom.xml into <plugin>(maven-javadoc-plugin) / " +
                        "<configuration> / <groups>");
                }

                // This limit is set for JDK11. Each group is represented as a tab. Tabs are enumerated with a number 2^N
                // where N is a sequential number for a tab. For 32 tabs (+ the "All Packages" tab) the number is overflowed
                // and the tabulation becomes broken. See var data in "index.html".
                if (pkgGrps > 30) {
                    throw new IllegalArgumentException("Too many package groups: " + pkgGrps + ". The limit"
                        + " is 30 due to the javadoc limitations. Please reduce groups in parent/pom.xml"
                        + " inside <plugin>(maven-javadoc-plugin) / <configuration> / <groups>");
                }
            }
            else if (!isViewHtml(file)) {
                // TODO: fix the description block location IGNITE-22650
                // Try to find a class description block.
                Jerry descBlock = doc.find("div.contentContainer .description");

                if (descBlock.size() == 0)
                    throw new IllegalArgumentException("Class doesn't have description in file: " + file);
            }
        }

        String s = fileContent.replaceFirst(
            "</head>",
            "<link rel='shortcut icon' href='https://ignite.apache.org/favicon.ico'/>\n</head>\n");

        replaceFile(file, s);
    }

    /**
     * Checks whether a file is a view-related HTML file rather than a single
     * class documentation.
     *
     * @param file HTML file.
     * @return {@code True} if it's a view-related HTML.
     */
    private boolean isViewHtml(File file) {
        String baseName = file.getName();

        return "index.html".equals(baseName) || baseName.contains("-") || "allclasses.html".equals(baseName);
    }

    /**
     * Replaces file with given body.
     *
     * @param file File to replace.
     * @param body New body for the file.
     * @throws IOException Thrown in case of any errors.
     */
    private void replaceFile(File file, String body) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            out.write(body.getBytes());
        }
    }

    /**
     * Reads file to string using specified charset.
     *
     * @param file File.
     * @return File content.
     * @throws IOException If error occurred.
     */
    public static String readFileToString(File file) throws IOException {
        Reader input = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);

        StringWriter output = new StringWriter();

        char[] buf = new char[4096];

        int n;

        while ((n = input.read(buf)) != -1)
            output.write(buf, 0, n);

        return output.toString();
    }
}
