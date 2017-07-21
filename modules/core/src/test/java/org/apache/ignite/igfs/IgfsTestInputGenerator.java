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

package org.apache.ignite.igfs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class IgfsTestInputGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Run: IgfsTestInputGenerator <file name> <length>");
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