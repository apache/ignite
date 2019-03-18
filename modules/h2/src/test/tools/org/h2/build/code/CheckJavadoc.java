/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.code;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * This tool checks that for each .java file there is a package.html file,
 * that for each .java file there is at least one (class level) javadoc comment,
 * and that lines with comments are not too long.
 */
public class CheckJavadoc {

    private static final int MAX_COMMENT_LINE_SIZE = 100;
    private static final int MAX_SOURCE_LINE_SIZE = 120;
    private int errorCount;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new CheckJavadoc().run();
    }

    private void run() throws Exception {
        String baseDir = "src";
        check(new File(baseDir));
        if (errorCount > 0) {
            throw new Exception(errorCount + " errors found");
        }
    }

    private int check(File file) throws Exception {
        String name = file.getName();
        if (file.isDirectory()) {
            if (name.equals("CVS") || name.equals(".svn")) {
                return 0;
            }
            boolean foundPackageHtml = false, foundJava = false;
            for (File f  : file.listFiles()) {
                int type = check(f);
                if (type == 1) {
                    foundJava = true;
                } else if (type == 2) {
                    foundPackageHtml = true;
                }
            }
            if (foundJava && !foundPackageHtml) {
                System.out.println(
                        "No package.html file, but a Java file found at: "
                        + file.getAbsolutePath());
                errorCount++;
            }
        } else {
            if (name.endsWith(".java")) {
                checkJavadoc(file);
                return 1;
            } else if (name.equals("package.html")) {
                return 2;
            }
        }
        return 0;
    }

    private void checkJavadoc(File file) throws IOException {
        RandomAccessFile in = new RandomAccessFile(file, "r");
        byte[] data = new byte[(int) file.length()];
        in.readFully(data);
        in.close();
        String text = new String(data);
        int comment = text.indexOf("/**");
        if (comment < 0) {
            System.out.println("No Javadoc comment: " + file.getAbsolutePath());
            errorCount++;
        }
        int pos = 0;
        int lineNumber = 1;
        boolean inComment = false;
        while (true) {
            int next = text.indexOf('\n', pos);
            if (next < 0) {
                break;
            }
            String rawLine = text.substring(pos, next);
            if (rawLine.endsWith("\r")) {
                rawLine = rawLine.substring(0, rawLine.length() - 1);
            }
            String line = rawLine.trim();
            if (line.startsWith("/*")) {
                inComment = true;
            }
            if (inComment) {
                if (rawLine.length() > MAX_COMMENT_LINE_SIZE
                        && !line.trim().startsWith("* http://")
                        && !line.trim().startsWith("* https://")) {
                    System.out.println("Long line: " + file.getAbsolutePath()
                            + " (" + file.getName() + ":" + lineNumber + ")");
                    errorCount++;
                }
                if (line.endsWith("*/")) {
                    inComment = false;
                }
            }
            if (!inComment && line.startsWith("//")) {
                if (rawLine.length() > MAX_COMMENT_LINE_SIZE
                        && !line.trim().startsWith("// http://")
                        && !line.trim().startsWith("// https://")) {
                    System.out.println("Long line: " + file.getAbsolutePath()
                            + " (" + file.getName() + ":" + lineNumber + ")");
                    errorCount++;
                }
            } else if (!inComment && rawLine.length() > MAX_SOURCE_LINE_SIZE) {
                System.out.println("Long line: " + file.getAbsolutePath()
                        + " (" + file.getName() + ":" + lineNumber + ")");
                errorCount++;
            }
            lineNumber++;
            pos = next + 1;
        }
    }

}
