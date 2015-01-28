package org.apache.ignite.internal.util.ipc.shmem;

import junit.framework.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

public class GridIpcSharedMemoryNativeLoaderSelfTest extends TestCase {

    public void testLoadWithCorruptedLibFile() throws Exception {
        if (U.isWindows())
            return;

        Process ps = GridJavaProcess.exec(
            LoadWithCorruptedLibFileTestRunner.class,
            null,
            null,
            null,
            null,
            Collections.<String>emptyList(),
            null
        ).getProcess();

        readStreams(ps);

        int code = ps.waitFor();

        assertEquals("Returned code have to be 0.", 0, code);
    }

    private void readStreams(Process proc) throws IOException {
        BufferedReader stdOut = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        String s;

        while ((s = stdOut.readLine()) != null)
            System.out.println("OUT>>>>>> " + s);

        BufferedReader errOut = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        while ((s = errOut.readLine()) != null)
            System.out.println("ERR>>>>>> " + s);
    }
}
