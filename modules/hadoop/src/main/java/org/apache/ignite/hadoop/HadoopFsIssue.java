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

package org.apache.ignite.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.ignite.internal.processors.hadoop.SecondaryFileSystemProvider;

/**
 * Comment.
 */
public class HadoopFsIssue {
    /**
     *
     * @param args
     */
    public static void main(String args[]) {
        String uri = null;
        String cfgPath = null;
        String user = null;

        for (String arg : args) {
            if (arg.startsWith("uri="))
                uri = arg.split("=")[1].trim();
            else if (arg.startsWith("cfg="))
                cfgPath = arg.split("=")[1].trim();
            else if (arg.startsWith("user="))
                user = arg.split("=")[1].trim();
            else
                throw new IllegalArgumentException("Unknown argument:" + arg);
        }

        System.out.println("Connecting to HDFS with the following settings [uri=" + uri + ", cfg=" + cfgPath + ", user=" + user + ']');

        try {
            SecondaryFileSystemProvider provider = new SecondaryFileSystemProvider(uri, cfgPath);

            FileSystem fs = provider.createFileSystem(user);

            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/tmp"), true);

            System.out.println("Got the iterator");

            while (iter.hasNext()) {
                LocatedFileStatus status = iter.next();

                System.out.println(status);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
