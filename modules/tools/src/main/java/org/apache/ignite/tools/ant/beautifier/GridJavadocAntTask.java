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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import jodd.jerry.Jerry;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.MatchingTask;

/**
 * Ant task fixing known HTML issues for Javadoc.
 */
public class GridJavadocAntTask extends MatchingTask {
    /** */
    private static final String SH_URL = "http://agorbatchev.typepad.com/pub/sh/3_0_83";

    /** Directory. */
    private File dir;

    /** CSS file name. */
    private String css;

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
     * Sets CSS file name.
     *
     * @param css CSS file name to set.
     */
    public void setCss(String css) {
        assert css != null;

        this.css = css;
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

        if (css == null)
            throw new BuildException("'css' attribute must be specified.");

        log("dir=" + dir, Project.MSG_DEBUG);
        log("css=" + css, Project.MSG_DEBUG);

        DirectoryScanner scanner = getDirectoryScanner(dir);

        boolean fail = false;

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

                fail = true;
            }
        }

        if (fail)
            throw new BuildException("Execution failed due to previous errors.");
    }

    /**
     * Processes file (validating and cleaning up Javadoc's HTML).
     *
     * @param file File to cleanup.
     * @throws IOException Thrown in case of any I/O error.
     * @throws IllegalArgumentException In JavaDoc HTML validation failed.
     */
    private void processFile(String file) throws IOException {
        assert file != null;

        String fileContent = readFileToString(file, Charset.forName("UTF-8"));

        if (verify) {
            // Parse HTML.
            Jerry doc = Jerry.jerry(fileContent);

            if (file.endsWith("overview-summary.html")) {
                // Try to find Other Packages section.
                Jerry otherPackages =
                    doc.find("div.contentContainer table.overviewSummary caption span:contains('Other Packages')");

                if (otherPackages.size() > 0)
                    throw new IllegalArgumentException("'Other Packages' section should not be present, " +
                        "all packages should have corresponding documentation groups: " + file);
            }
            else if (!isViewHtml(file)) {
                // Try to find a class description block.
                Jerry descBlock = doc.find("div.contentContainer div.description ul.blockList li.blockList div.block");

                if (descBlock.size() == 0)
                    throw new IllegalArgumentException("Class doesn't have description in file: " + file);
            }
        }

        GridJavadocCharArrayLexReader lexer = new GridJavadocCharArrayLexReader(fileContent.toCharArray());

        Collection<GridJavadocToken> toks = new ArrayList<>();

        StringBuilder tokBuf = new StringBuilder();

        int ch;

        while ((ch = lexer.read()) != GridJavadocCharArrayLexReader.EOF) {
            // Instruction, tag or comment.
            if (ch =='<') {
                if (tokBuf.length() > 0) {
                    toks.add(new GridJavadocToken(GridJavadocTokenType.TOKEN_TEXT, tokBuf.toString()));

                    tokBuf.setLength(0);
                }

                tokBuf.append('<');

                ch = lexer.read();

                if (ch == GridJavadocCharArrayLexReader.EOF)
                    throw new IOException("Unexpected EOF: " + file);

                // Instruction or comment.
                if (ch == '!') {
                    for (; ch != GridJavadocCharArrayLexReader.EOF && ch != '>'; ch = lexer.read())
                        tokBuf.append((char)ch);

                    if (ch == GridJavadocCharArrayLexReader.EOF)
                        throw new IOException("Unexpected EOF: " + file);

                    assert ch == '>';

                    tokBuf.append('>');

                    String val = tokBuf.toString();

                    toks.add(new GridJavadocToken(val.startsWith("<!--") ? GridJavadocTokenType.TOKEN_COMM :
                        GridJavadocTokenType.TOKEN_INSTR, val));

                    tokBuf.setLength(0);
                }
                // Tag.
                else {
                    for (; ch != GridJavadocCharArrayLexReader.EOF && ch != '>'; ch = lexer.read())
                        tokBuf.append((char)ch);

                    if (ch == GridJavadocCharArrayLexReader.EOF)
                        throw new IOException("Unexpected EOF: " + file);

                    assert ch == '>';

                    tokBuf.append('>');

                    if (tokBuf.length() <= 2)
                        throw new IOException("Invalid HTML in [file=" + file + ", html=" + tokBuf + ']');

                    String val = tokBuf.toString();

                    toks.add(new GridJavadocToken(val.startsWith("</") ?
                        GridJavadocTokenType.TOKEN_CLOSE_TAG : GridJavadocTokenType.TOKEN_OPEN_TAG, val));

                    tokBuf.setLength(0);
                }
            }
            else
                tokBuf.append((char)ch);
        }

        if (tokBuf.length() > 0)
            toks.add(new GridJavadocToken(GridJavadocTokenType.TOKEN_TEXT, tokBuf.toString()));

        for (GridJavadocToken tok : toks) {
            String val = tok.value();

            switch (tok.type()) {
                case TOKEN_COMM: {
                    break;
                }

                case TOKEN_OPEN_TAG: {
                    tok.update(fixColors(tok.value()));

                    break;
                }

                case TOKEN_CLOSE_TAG: {
                    if ("</head>".equalsIgnoreCase(val))
                        tok.update(
                            "<link rel='shortcut icon' href='https://ignite.apache.org/favicon.ico'/>\n" +
                            "<link type='text/css' rel='stylesheet' href='" + SH_URL + "/styles/shCore.css'/>\n" +
                            "<link type='text/css' rel='stylesheet' href='" + SH_URL +
                                "/styles/shThemeDefault.css'/>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shCore.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shLegacy.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shBrushJava.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shBrushPlain.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL +
                                "/scripts/shBrushJScript.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shBrushBash.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shBrushXml.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shBrushScala.js'></script>\n" +
                            "<script type='text/javascript' src='" + SH_URL + "/scripts/shBrushGroovy.js'></script>\n" +
                            "</head>\n");
                    else if ("</body>".equalsIgnoreCase(val))
                        tok.update(
                            "<!--FOOTER-->" +
                            "<script type='text/javascript'>" +
                                "SyntaxHighlighter.all();" +
                                "dp.SyntaxHighlighter.HighlightAll('code');" +
                            "</script>\n" +
                            "</body>\n");

                    break;
                }

                case TOKEN_INSTR: {
                    // No-op.

                    break;
                }

                case TOKEN_TEXT: {
                    tok.update(fixColors(val));

                    break;
                }

                default:
                    assert false;
            }
        }

        StringBuilder buf = new StringBuilder();
        StringBuilder tmp = new StringBuilder();

        boolean inPre = false;

        // Second pass for unstructured replacements.
        for (GridJavadocToken tok : toks) {
            String val = tok.value();

            switch (tok.type()) {
                case TOKEN_INSTR:
                case TOKEN_TEXT:
                case TOKEN_COMM: {
                    tmp.append(val);

                    break;
                }

                case TOKEN_OPEN_TAG: {
                    if (val.toLowerCase().startsWith("<pre name=")) {
                        inPre = true;

                        buf.append(fixBrackets(tmp.toString()));

                        tmp.setLength(0);
                    }

                    tmp.append(val);

                    break;
                }

                case TOKEN_CLOSE_TAG: {
                    if (val.toLowerCase().startsWith("</pre") && inPre) {
                        inPre = false;

                        buf.append(tmp.toString());

                        tmp.setLength(0);
                    }

                    tmp.append(val);

                    break;
                }

                default:
                    assert false;
            }
        }

        String s = buf.append(fixBrackets(tmp.toString())).toString();

        s = fixExternalLinks(s);
        s = fixDeprecated(s);
        s = fixNullable(s);
        s = fixTodo(s);

        replaceFile(file, s);
    }

    /**
     * Checks whether a file is a view-related HTML file rather than a single
     * class documentation.
     *
     * @param fileName HTML file name.
     * @return {@code True} if it's a view-related HTML.
     */
    private boolean isViewHtml(String fileName) {
        String baseName = new File(fileName).getName();

        return "index.html".equals(baseName) || baseName.contains("-");
    }

    /**
     *
     * @param s String token.
     * @return Token with replaced colors.
     */
    private String fixColors(String s) {
        return s.replace("0000c0", "000000").
            replace("000000", "333333").
            replace("c00000", "333333").
            replace("008000", "999999").
            replace("990000", "336699").
            replace("font color=\"#808080\"", "font size=-2 color=\"#aaaaaa\"");
    }

    /**
     *
     * @param s String token.
     * @return Fixed token value.
     */
    private String fixBrackets(String s) {
        return s.replace("&lt;", "<span class='angle_bracket'>&lt;</span>").
            replace("&gt;", "<span class='angle_bracket'>&gt;</span>");
    }

    /**
     *
     * @param s String token.
     * @return Fixed token value.
     */
    private String fixTodo(String s) {
        return s.replace("TODO", "<span class='todo'>TODO</span>");
    }

    /**
     *
     * @param s String token.
     * @return Fixed token value.
     */
    private String fixNullable(String s) {
        return s.replace("<FONT SIZE=\"-1\">@Nullable", "<FONT SIZE=\"-1\" class='nullable'>@Nullable");
    }

    /**
     *
     * @param s String token.
     * @return Fixed token value.
     */
    private String fixDeprecated(String s) {
        return s.replace("<B>Deprecated.</B>", "<span class='deprecated'>Deprecated.</span>");
    }

    /**
     *
     * @param s String token.
     * @return Fixed token value.
     */
    private String fixExternalLinks(String s) {
        return s.replace("A HREF=\"http://java.sun.com/j2se/1.6.0",
            "A target='jse5javadoc' HREF=\"http://java.sun.com/j2se/1.6.0");
    }

    /**
     * Replaces file with given body.
     *
     * @param file File to replace.
     * @param body New body for the file.
     * @throws IOException Thrown in case of any errors.
     */
    private void replaceFile(String file, String body) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            out.write(body.getBytes());
        }
    }

    /**
     * Reads file to string using specified charset.
     *
     * @param fileName File name.
     * @param charset File charset.
     * @return File content.
     * @throws IOException If error occurred.
     */
    public static String readFileToString(String fileName, Charset charset) throws IOException {
        Reader input = new InputStreamReader(new FileInputStream(fileName), charset);

        StringWriter output = new StringWriter();

        char[] buf = new char[4096];

        int n;

        while ((n = input.read(buf)) != -1)
            output.write(buf, 0, n);

        return output.toString();
    }
}