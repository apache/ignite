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

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Created by gridgain on 24.04.2017.
 */
public class IgniteKernalLogTest extends GridCommonAbstractTest implements IgniteInClosure<String> {
    public static String prefix = IgniteKernalLogMain.prefix;

    Boolean includeSensitive = null;

    public void testNone() throws Exception {
       runIgniteProcess(null);
        //IgniteKernalLogTest.main();
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
       //             "-v");

        GridJavaProcess proc = null;

        try {
            proc = GridJavaProcess.exec(
                    IgniteKernalLogMain.class,
                    null, // Params.
                    this.log,
                    // Optional closure to be called each time wrapped process prints line to system.out or system.err.
                    this,
                    null,
                    jvmArgs,
                    null
            );
            int exitCode = proc.getProcess().waitFor();

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

        System.out.println(s);

        if (s.startsWith(prefix)) {
            s=s.substring(prefix.length());
            includeSensitive = Boolean.parseBoolean(s);
        }
    }
}
