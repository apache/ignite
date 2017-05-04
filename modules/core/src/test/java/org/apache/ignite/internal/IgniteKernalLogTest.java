package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Created by gridgain on 24.04.2017.
 */
public class IgniteKernalLogTest extends GridCommonAbstractTest implements IgniteInClosure<String> {
    public static String prefix = "S.INCLUDE_SENSITIVE=";

    Boolean includeSensitive = null;

    public void testNone() throws Exception {
       runIgniteProcess(null);
    }

    public void testOff() throws Exception {
       runIgniteProcess(false);
    }

    public void testOn() throws Exception {
        runIgniteProcess(true);
    }

    public void runIgniteProcess(Boolean on) throws Exception {
        assertNull(includeSensitive);
        Collection<String> jvmArgs = null;
        if (on == null)
            on = true;
        else
            jvmArgs = Arrays.asList("-D"+IGNITE_TO_STRING_INCLUDE_SENSITIVE+"="+on.toString(),
                    "-DIGNITE_QUIET=false");

        GridJavaProcess proc = null;

        try {
            proc = GridJavaProcess.exec(
                    IgniteKernalLogTest.class,
                    null, // Params.
                    this.log,
                    // Optional closure to be called each time wrapped process prints line to system.out or system.err.
                    this,
                    null,
                    jvmArgs,
                    null
            );

            Process process = proc.getProcess();

            int timeout = 20; //60

            boolean finished = process.waitFor(timeout, TimeUnit.SECONDS);

            if (!finished) {
                proc.kill();
                System.out.println("process timeout:" + timeout + " seconds");
            }

            int exitCode = process.waitFor();

            assertEquals("Unexpected exit code", 0, exitCode);
        }
        finally {
            if (proc != null)
                proc.killProcess();

        }
        assertNotNull(includeSensitive);

        assertEquals(on, includeSensitive);
    }

    @Override
    public void apply(String s) {

        System.out.print("(proc)");

        System.out.println(s);

        if (s.startsWith(prefix)) {

            s = s.substring(prefix.length());

            includeSensitive = Boolean.parseBoolean(s);
        }
    }

    public static void main(String... args) {
        System.out.println(prefix + S.INCLUDE_SENSITIVE);
    }
}
