package org.apache.ignite.cli.ui;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** */
public class ProgressBarTest {
    /** */
    private PrintWriter out;

    /** */
    private ByteArrayOutputStream outputStream;

    /** */
    @BeforeEach
    private void setUp() {
        outputStream = new ByteArrayOutputStream();
        out = new PrintWriter(outputStream, true);
    }

    /** */
    @AfterEach
    private void tearDown() throws IOException {
        out.close();
        outputStream.close();
    }

    /** */
    @Test
    public void testScaledToTerminalWidth() throws IOException {
        var progressBar = new ProgressBar(out, 3, 80);
        progressBar.step();
        assertEquals(80, outputStream.toString().replace("\r", "").length());
        assertEquals(
            "\r|========================>                                                 | 33%",
            outputStream.toString()
        );
    }

    /** */
    @Test
    public void testRedundantStepsProgressBar() {
        var progressBar = new ProgressBar(out, 3, 80);
        progressBar.step();
        progressBar.step();
        progressBar.step();
        progressBar.step();
        assertEquals(
            "\r|========================>                                                 | 33%" +
                "\r|================================================>                         | 66%" +
                "\r|==========================================================================|Done!" +
                "\r|==========================================================================|Done!",
            outputStream.toString()
        );
    }
}
