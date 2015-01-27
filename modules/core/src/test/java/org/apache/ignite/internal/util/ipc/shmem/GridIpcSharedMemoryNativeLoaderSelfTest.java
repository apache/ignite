package org.apache.ignite.internal.util.ipc.shmem;

import junit.framework.TestCase;
import org.apache.ignite.internal.util.GridJavaProcess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;

public class GridIpcSharedMemoryNativeLoaderSelfTest extends TestCase {
    //TODO linux specific
    public void testLoadIfLibFileWasCorrupted() throws Exception {
        Process ps = GridJavaProcess.exec(LoadIfLibFileWasCorruptedTestRunner.class, null, null, null, null, Collections.<String>emptyList(), null).getProcess();

        readStreams(ps);
        int code = ps.waitFor();

        assertEquals("Returned code have to be 0.", 0, code);
    }

    private void readStreams(Process proc) throws IOException {
        BufferedReader stdOut = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String s;
        while ((s = stdOut.readLine()) != null) {
            System.out.println("OUT>>>>>> " + s);
        }

        BufferedReader errOut = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        while ((s = errOut.readLine()) != null) {
            System.out.println("ERR>>>>>> " + s);
        }
    }
}