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