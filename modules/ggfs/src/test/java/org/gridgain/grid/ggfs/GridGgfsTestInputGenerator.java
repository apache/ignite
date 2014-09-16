/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import java.io.*;

/**
 *
 */
public class GridGgfsTestInputGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Run: GridGgfsTestInputGenerator <file name> <length>");
            System.exit(-1);
        }

        String outFileName = args[0];

        long len = Long.parseLong(args[1]);

        long start = System.currentTimeMillis();

        OutputStream out = new BufferedOutputStream(new FileOutputStream(outFileName), 32*1024*1024);

        for (long i = 0; i < len; i++)
                out.write(read(i));

        out.close();

        System.out.println("Finished in: " + (System.currentTimeMillis() - start));
    }

    private static int read(long pos) {
        return (int)(pos % 116) + 10;
    }
}
