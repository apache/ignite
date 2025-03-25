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

package org.apache.ignite.internal.management.cache;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class CacheCreateCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(description = "Path to the Spring XML configuration that contains " +
        "'org.apache.ignite.configuration.CacheConfiguration' beans to create caches from", example = "springXmlConfigPath")
    private String springxmlconfig;

    /** */
    @Argument(description = "Optional flag to skip existing caches", optional = true)
    private boolean skipExisting;

    /** */
    private String fileContent;

    /** */
    private void readFile() {
        if (!new File(springxmlconfig).exists()) {
            throw new IgniteException("Failed to create caches. Spring XML configuration file not found " +
                "[file=" + springxmlconfig + ']');
        }

        try {
            fileContent = U.readFileToString(springxmlconfig, "UTF-8");
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create caches. Failed to read Spring XML configuration file " +
                "[file=" + springxmlconfig + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, springxmlconfig);
        U.writeString(out, fileContent);
        out.writeBoolean(skipExisting);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        springxmlconfig = U.readString(in);
        fileContent = U.readString(in);
        skipExisting = in.readBoolean();
    }

    /** */
    public String springxmlconfig() {
        return springxmlconfig;
    }

    /** */
    public void springxmlconfig(String springxmlconfig) {
        this.springxmlconfig = springxmlconfig;
        readFile();
    }

    /** */
    public boolean skipExisting() {
        return skipExisting;
    }

    /** */
    public void skipExisting(boolean skipExisting) {
        this.skipExisting = skipExisting;
    }

    /** */
    public void fileContent(String fileContent) {
        this.fileContent = fileContent;
    }

    /** */
    public String fileContent() {
        return fileContent;
    }
}
