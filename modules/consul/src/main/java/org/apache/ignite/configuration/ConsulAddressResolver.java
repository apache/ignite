package org.apache.ignite.configuration;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;

import java.net.InetSocketAddress;
import java.util.*;

public class ConsulAddressResolver implements AddressResolver {
    public static final String DELIM = ".";
    ConsulClient consulClient;
    String hostUrl;
    String serviceName = "ignite";
    boolean doCache = true;
    private Map<InetSocketAddress, InetSocketAddress> inetSockAddrMap = new HashMap<>();
    /**
     * Grid logger.
     */
    @LoggerResource
    private IgniteLogger log;

    public ConsulAddressResolver(String hostUrl, String serviceName, boolean doCache) {
        this.hostUrl = hostUrl;
        this.serviceName = serviceName;
        this.consulClient = new ConsulClient(this.hostUrl);
        this.doCache = doCache;
    }

    public ConsulAddressResolver(String hostUrl) {
        this(hostUrl, "ignite", true);
    }

    public ConsulAddressResolver(String hostUrl, String serviceName) {
        this(hostUrl, serviceName, true);
    }

    private InetSocketAddress getResolvedAddress(InetSocketAddress inetSocketAddress) {
        InetSocketAddress mapAddress = null;
        if (doCache) {
            mapAddress = inetSockAddrMap.get(inetSocketAddress);
        }

        if (!Optional.of(mapAddress).isPresent()) {
            try {
                Response<GetValue> value = consulClient.getKVValue(key(inetSocketAddress));
                String ipAddress = value.getValue().getValue();
                mapAddress = InetSocketAddress.createUnresolved(ipAddress, inetSocketAddress.getPort());
                if (doCache)
                    inetSockAddrMap.put(inetSocketAddress, mapAddress);
            } catch (Exception e) {
                throw new IgniteSpiException("get Value from consul service : ", e);
            }
        }
        return mapAddress;
    }

    @Override
    public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress inetSocketAddress) {
        return Collections.singleton(getResolvedAddress(inetSocketAddress));
    }

    /**
     * Gets key for provided address.
     *
     * @param addr Node address.
     * @return Key.
     */
    private String key(InetSocketAddress addr) {
        assert addr != null;

        SB sb = new SB();

        sb.a(this.serviceName)
                .a(DELIM).a(addr.getAddress()
                .getHostAddress())
                .a(DELIM)
                .a(addr.getPort());

        return sb.toString();
    }
}
