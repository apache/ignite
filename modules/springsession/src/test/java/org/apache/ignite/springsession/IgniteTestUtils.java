package org.apache.ignite.springsession;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class IgniteTestUtils {

	public static Ignite getIgniteServerInstance() {
		return Ignition.start(getIgniteCfg().setIgniteInstanceName("server"));
	}

	public static Ignite getIgniteClientInstance() {
		return Ignition.start(getIgniteCfg().setClientMode(true).setIgniteInstanceName("client"));
	}

	private static IgniteConfiguration getIgniteCfg() {
		IgniteConfiguration cfg = new IgniteConfiguration();

		TcpDiscoverySpi discovery = new TcpDiscoverySpi();
		TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
		finder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
		discovery.setIpFinder(finder);
		cfg.setDiscoverySpi(discovery);

		return cfg;
	}
}
