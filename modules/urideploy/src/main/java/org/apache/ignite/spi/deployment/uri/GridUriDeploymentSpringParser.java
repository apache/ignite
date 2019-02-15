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

package org.apache.ignite.spi.deployment.uri;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.InputStreamResource;

/**
 * Workaround for {@link InputStreamResource}. Converts input stream with XML
 * to {@code GridUriDeploymentSpringDocument} with {@link ByteArrayResource}
 * instead of {@link InputStreamResource}.
 */
final class GridUriDeploymentSpringParser {
    /**
     * Enforces singleton.
     */
    private GridUriDeploymentSpringParser() {
        // No-op.
    }

    /**
     * Converts given input stream expecting XML inside to
     * {@link GridUriDeploymentSpringDocument}.
     * <p>
     * This is a workaround for the {@link InputStreamResource} which does
     * not work properly.
     *
     * @param in Input stream with XML.
     * @param log Logger
     * @return Grid wrapper for the input stream.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if incoming input stream could not be
     *      read or parsed by {@code Spring} {@link XmlBeanFactory}.
     * @see XmlBeanFactory
     */
    static GridUriDeploymentSpringDocument parseTasksDocument(InputStream in, IgniteLogger log) throws
        IgniteSpiException {
        assert in != null;

        // Note: use ByteArrayResource instead of InputStreamResource because InputStreamResource doesn't work.
        ByteArrayOutputStream out  = new ByteArrayOutputStream();

        try {
            U.copy(in, out);

            XmlBeanFactory factory = new XmlBeanFactory(new ByteArrayResource(out.toByteArray()));

            return new GridUriDeploymentSpringDocument(factory);
        }
        catch (BeansException | IOException e) {
            throw new IgniteSpiException("Failed to parse spring XML file.", e);
        }
        finally{
            U.close(out, log);
        }
    }
}