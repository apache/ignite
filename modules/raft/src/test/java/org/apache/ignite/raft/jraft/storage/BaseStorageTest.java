/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;

public class BaseStorageTest {
    protected String path;

    public void setup() throws Exception {
        this.path = TestUtils.mkTempDir();
        new File(this.path).mkdirs();
    }

    @After
    public void teardown() throws Exception {
        Utils.delete(new File(this.path));
    }

    protected String writeData() throws IOException {
        File file = new File(this.path + File.separator + "data");
        String data = "jraft is great!";
        Files.writeString(file.toPath(), data);
        return data;
    }

}
