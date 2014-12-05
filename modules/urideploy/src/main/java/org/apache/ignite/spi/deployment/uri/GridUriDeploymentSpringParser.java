/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.apache.ignite.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.core.io.*;
import java.io.*;

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
