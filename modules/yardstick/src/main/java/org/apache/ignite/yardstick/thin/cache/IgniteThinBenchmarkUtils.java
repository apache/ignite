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
