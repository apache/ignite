// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import java.util.*;

/**
 * This interface defines that an SPI instance can be configured via JSON. This interface
 * is optional and can be extended by SPI interfaces only. Note that JSON configuration is not
 * exclusive for an SPI and any configuration parameters can be configured via regular setters
 * or Spring XML (in addition to or instead of JSON configuration). Note also that if property
 * was set before the JSON configuration is invoked it will override the previously set values
 * (and vice-versa).
 * <p>
 * The main idea behind JSON-based configuration is to provide more terse configuration via JSON
 * rather than traditional Spring syntax.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridSpiJsonConfigurable {
    /**
     * Sets JSON configuration string and injects the properties defined in it into
     * SPI instance that implements this interface.
     * <p>
     * JSON specification is available at <a target=_ href="http://www.json.org">http://www.json.org</a>.
     * Additionally to the standard JSON there are two special properties:
     * <ol>
     * <li>
     *      To specify the type of the object you can use {@code &#64;class} property specifying fully
     *      qualified class name. If type is not specified implementation will try to infer the type
     *      via reflection. If type is not specified and cannot be determined via reflection, the
     *      implementation will use {@link HashMap} for {@code net.sf.json.JSONObject} and
     *      {@link ArrayList} for {@code net.sf.json.JSONArray}.
     * </li>
     * <li>
     *      To specify element type of collection you can use {@code &#64;elem} property providing fully
     *      qualified class name. If type is not specified implementation will try to infer the type
     *      via reflection. If type is not specified and cannot be determined via reflection, the
     *      implementation will use {@link HashMap} for {@code net.sf.json.JSONObject} and
     *      {@link ArrayList} for {@code net.sf.json.JSONArray}.
     * </li>
     * </ol>
     * <p>
     * This example defines one {@link HashMap} with the only key {@code key} mapped to
     * {@link LinkedHashMap} object that will have two entries with the keys {@code a0} and {@code a1}.
     * The mapped values will be of type {@code foo.bar.A} which has settable property {@code val}:
     * <pre name="code" class="js">
     *  {
     *      key: {
     *          a0: {val:'test0'},
     *          a1: {val:'test1'},
     *          &#64;class:'java.util.LinkedHashMap',
     *          &#64;elem:'foo.bar.A'
     *      }
     *  }
     * </pre>
     * <p>
     * Note that you can see more examples on JSON configuration for specific SPI in default Spring
     * XML configuration file {@code ${GRIDGAIN_HOME}/config/default-config.xml}
     * <p>
     * The following example shows usage of JSON configuration in Spring XML:
     * <pre name="code" class="xml">
     *    &lt;property name=&quot;communicationSpi&quot;&gt;
     *        &lt;bean class=&quot;org.gridgain.grid.spi.communication.tcp.GridTcpCommunicationSpi&quot;&gt;
     *            &lt;property name=&quot;json&quot;
     *                value=&quot;{directBuffer: &#39;false&#39;, localAddress=&#39;1.2.3.4&#39;, localPort: 47100, localPortRange: 100}&quot;/&gt;
     *        &lt;/bean&gt;
     *    &lt;/property&gt;
     * </pre>
     *
     * @param json JSON expression.
     */
    public void setJson(String json);
}
