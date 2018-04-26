package org.apache.ignite.yardstick.thin.cache;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 */
public class IgniteThinBenchmarkUtils {
    /**
     *
     * @param cfg
     * @return
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
     *
     * @param cfg
     * @return
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

    public static String[] servHostArr(BenchmarkConfiguration cfg){
        String servHosts = cfg.customProperties().get("SERVER_HOSTS");

        return servHosts.split(",");

    }
}
