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

package org.apache.ignite.ml;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.IgniteLogger;

/**
 * Implementation of exporter to/from file.
 *
 * Default implementation of {@link Exporter}.
 */
public class FileExporter<D> implements Exporter<D, String> {
    /** */
    private IgniteLogger log;

    /**
     * @param log Logger.
     */
    public void setLog(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void save(D d, String path) {
        try (FileOutputStream fos = new FileOutputStream(path)) {
            try (ObjectOutputStream outStream = new ObjectOutputStream(fos)) {
                outStream.writeObject(d);
            }
        }
        catch (IOException e) {
            if (log != null)
                log.error("Error opening file.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public D load(String path) {
        D mdl = null;

        try (FileInputStream fis = new FileInputStream(path)) {
            try (ObjectInputStream inputStream = new ObjectInputStream(fis)) {
                mdl = (D)inputStream.readObject();
            }
            catch (ClassNotFoundException e) {
                if (log != null)
                    log.error("Object creation failed.", e);
            }
        }
        catch (IOException e) {
            if (log != null)
                log.error("Error opening file.", e);
        }
        return mdl;
    }
}
