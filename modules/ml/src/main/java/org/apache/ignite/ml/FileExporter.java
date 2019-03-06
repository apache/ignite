/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    private IgniteLogger log = null;

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
