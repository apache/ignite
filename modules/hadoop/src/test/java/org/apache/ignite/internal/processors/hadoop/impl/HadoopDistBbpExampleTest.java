package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.examples.BaileyBorweinPlouffe;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

/**
 * Distributed Bbp Pi digits example.
 */
public class HadoopDistBbpExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final BaileyBorweinPlouffe impl = new BaileyBorweinPlouffe();

        @Override String[] parameters(FrameworkParameters fp) {
            // <startDigit> <nDigits> <nMaps> <workingDir> :
            return new String[] { "1", "120", String.valueOf(fp.numMaps()), fp.getWorkDir(name()) };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) throws Exception {
            final String dir = parameters[3];

            final Path txtFile = new Path(dir, "pi.txt");

            try (BufferedReader br = new BufferedReader(new InputStreamReader(getFileSystem().open(txtFile)))) {
                int cntPi = 0;

                boolean found = false;

                while (true) {
                    String line = br.readLine();

                    if (line == null)
                        break;

                    if (line.contains("Pi = ")) {
                        cntPi++;

                        if (cntPi == 2) {
                            found = true;

                            assertEquals("Pi = 3.1415926535 8979323846 2643383279 5028841971 6939937510", line.trim());
                            assertEquals("5820974944 5923078164 0628620899 8628034825 3421170679", br.readLine().trim
                                ());
                            assertEquals("8214808651 3282306647", br.readLine().trim());

                            break;
                        }
                    }
                }

                assertTrue(found);
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
