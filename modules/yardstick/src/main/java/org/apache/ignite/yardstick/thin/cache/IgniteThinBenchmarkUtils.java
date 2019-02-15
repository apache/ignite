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

package org.apache.ignite.yardstick.thin.cache;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Thin client benchmark utils.
 */
public class IgniteThinBenchmarkUtils {
    /**
     * Compute local IP address.
     *
     * @param cfg Configuration.
     * @return local IP address.
     * @throws SocketException
     */
    public static String getLocalIp(BenchmarkConfiguration cfg) throws SocketException {
        List<String> hostList = drvHostList(cfg);

        Enumeration e = NetworkInterface.getNetworkInterfaces();

        while(e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();

            Enumeration ee = n.getInetAddresses();

            while (ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();

                if(hostList.contains(i.getHostAddress()))
                    return i.getHostAddress();
            }
        }

        return null;
    }

    /**
     * Creates list of driver host addresses.
     *
     * @param cfg Configuration.
     * @return List of driver host addresses.
     */
    public static List<String> drvHostList(BenchmarkConfiguration cfg){
        String driverHosts = cfg.customProperties().get("DRIVER_HOSTS");

        String[] hostArr = driverHosts.split(",");

        List<String> res = new ArrayList<>(hostArr.length);

        for(String host : hostArr){
            if(host.equals("localhost"))
                res.add("127.0.0.1");
            else
                res.add(host);
        }

        return res;
    }

    /**
     * Creates array of server host addresses.
     *
     * @param cfg Configuration.
     * @return {@code Array} of server host addresses.
     */
    public static String[] servHostArr(BenchmarkConfiguration cfg){
        String servHosts = cfg.customProperties().get("SERVER_HOSTS");

        return servHosts.split(",");

    }
}
