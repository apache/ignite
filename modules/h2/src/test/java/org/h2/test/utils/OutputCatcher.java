/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * A tool to capture the output of System.out and System.err. The regular output
 * still occurs, but it is additionally available as a String.
 */
public class OutputCatcher {

    /**
     * The HTML text will contain this string if something was written to
     * System.err.
     */
    public static final String START_ERROR = "<span style=\"color:red;\">";

    private final ByteArrayOutputStream buff = new ByteArrayOutputStream();
    private final DualOutputStream out, err;
    private String output;

    private OutputCatcher() {
        HtmlOutputStream html = new HtmlOutputStream(buff);
        out = new DualOutputStream(html, System.out, false);
        err = new DualOutputStream(html, System.err, true);
        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(err, true));
    }

    /**
     * Stop catching output.
     */
    public void stop() {
        System.out.flush();
        System.setOut(out.print);
        System.err.flush();
        System.setErr(err.print);
        output = new String(buff.toByteArray());
    }

    /**
     * Write the output to a HTML file.
     *
     * @param title the title
     * @param fileName the file name
     */
    public void writeTo(String title, String fileName) throws IOException {
        File file = new File(fileName);
        file.getParentFile().mkdirs();
        PrintWriter writer = new PrintWriter(new FileOutputStream(file));
        writer.write("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 " +
                "Strict//EN\" " +
                "\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">\n");
        writer.write("<html xmlns=\"http://www.w3.org/1999/xhtml\" " +
                "lang=\"en\" xml:lang=\"en\">\n");
        writer.write("<head><meta http-equiv=\"Content-Type\" " +
                "content=\"text/html;charset=utf-8\" /><title>\n");
        writer.print(title);
        writer.print("</title><link rel=\"stylesheet\" " +
                "type=\"text/css\" href=\"stylesheet.css\" />\n");
        writer.print("</head><body style=\"margin: 20px;\">\n");
        writer.print("<h1>" + title + "</h1><br />\n");
        writer.print(output);
        writer.write("\n</body></html>");
        writer.close();
    }

    /**
     * Create a new output catcher and start it.
     *
     * @return the output catcher
     */
    public static OutputCatcher start() {
        return new OutputCatcher();
    }

    /**
     * An output stream that writes to both a HTML stream and a print stream.
     */
    static class DualOutputStream extends FilterOutputStream {

        /**
         * The original print stream.
         */
        final PrintStream print;

        private final HtmlOutputStream htmlOut;
        private final boolean error;

        DualOutputStream(HtmlOutputStream out, PrintStream print, boolean error) {
            super(out);
            this.htmlOut = out;
            this.print = print;
            this.error = error;
        }

        @Override
        public void close() throws IOException {
            print.close();
            super.close();
        }

        @Override
        public void flush() throws IOException {
            print.flush();
            super.flush();
        }

        @Override
        public void write(int b) throws IOException {
            print.write(b);
            htmlOut.write(error, b);
        }
    }

    /**
     * An output stream that has two modes: error mode and regular mode.
     */
    static class HtmlOutputStream extends FilterOutputStream {

        private static final byte[] START = START_ERROR.getBytes();
        private static final byte[] END = "</span>".getBytes();
        private static final byte[] BR = "<br />\n".getBytes();
        private static final byte[] NBSP = "&nbsp;".getBytes();
        private static final byte[] LT = "&lt;".getBytes();
        private static final byte[] GT = "&gt;".getBytes();
        private static final byte[] AMP = "&amp;".getBytes();
        private boolean error;
        private boolean hasError;
        private boolean convertSpace;

        HtmlOutputStream(OutputStream out) {
            super(out);
        }

        /**
         * Check if the error mode was used.
         *
         * @return true if it was
         */
        boolean hasError() {
            return hasError;
        }

        /**
         * Enable or disable the error mode.
         *
         * @param error the flag
         */
        void setError(boolean error) throws IOException {
            if (error != this.error) {
                if (error) {
                    hasError = true;
                    super.write(START);
                } else {
                    super.write(END);
                }
                this.error = error;
            }
        }

        /**
         * Write a character.
         *
         * @param errorStream if the character comes from the error stream
         * @param b the character
         */
        void write(boolean errorStream, int b) throws IOException {
            setError(errorStream);
            switch (b) {
            case '\n':
                super.write(BR);
                convertSpace = true;
                break;
            case '\t':
                super.write(NBSP);
                super.write(NBSP);
                break;
            case ' ':
                if (convertSpace) {
                    super.write(NBSP);
                } else {
                    super.write(b);
                }
                break;
            case '<':
                super.write(LT);
                break;
            case '>':
                super.write(GT);
                break;
            case '&':
                super.write(AMP);
                break;
            default:
                if (b >= 128) {
                    super.write(("&#" + b + ";").getBytes());
                } else {
                    super.write(b);
                }
                convertSpace = false;
            }
        }

    }

}
