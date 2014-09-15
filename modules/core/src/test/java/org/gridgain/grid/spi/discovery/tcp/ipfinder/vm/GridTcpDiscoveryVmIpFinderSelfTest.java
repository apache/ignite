/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder.vm;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;

import java.util.*;

/**
 * GridTcpDiscoveryVmIpFinder test.
 */
public class GridTcpDiscoveryVmIpFinderSelfTest
    extends GridTcpDiscoveryIpFinderAbstractSelfTest<GridTcpDiscoveryVmIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public GridTcpDiscoveryVmIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoveryVmIpFinder ipFinder() {
        GridTcpDiscoveryVmIpFinder finder = new GridTcpDiscoveryVmIpFinder();

        assert !finder.isShared() : "Ip finder should NOT be shared by default.";

        return finder;
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testAddressesInitialization() throws Exception {
        GridTcpDiscoveryVmIpFinder finder = ipFinder();

        try {
            finder.setAddresses(Arrays.asList("127.0.0.1:475000001"));

            assert false;
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);

            assert e.getMessage().contains("127.0.0.1:475000001");
        }

        try {
            finder.setAddresses(Arrays.asList("127.0.0.1:-2"));

            assert false;
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);

            assert e.getMessage().contains("127.0.0.1:-2");
        }

        finder.setAddresses(Arrays.asList("127.0.0.1:45555", "8.8.8.8", "some-dns-name", "some-dns-name1:200",
            "127.0.0.1:"));

        info("IP finder initialized: " + finder);

        assert finder.getRegisteredAddresses().size() == 5;

        finder = ipFinder();

        try {
            finder.setAddresses(Collections.singleton("127.0.0.1:555..444"));

            assert false;
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("127.0.0.1:0..444"));

            assert false;
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("127.0.0.1:-8080..-80"));

            assert false;
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        finder.setAddresses(Collections.singleton("127.0.0.1:47500..47509"));

        assert finder.getRegisteredAddresses().size() == 10 : finder.getRegisteredAddresses();
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testIpV6AddressesInitialization() throws Exception {
        GridTcpDiscoveryVmIpFinder finder = ipFinder();

        try {
            finder.setAddresses(Arrays.asList("[::1]:475000001"));

            fail();
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Arrays.asList("[::1]:-2"));

            fail();
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        finder.setAddresses(Arrays.asList("[::1]:45555", "8.8.8.8", "some-dns-name", "some-dns-name1:200", "::1"));

        info("IP finder initialized: " + finder);

        assertEquals(5, finder.getRegisteredAddresses().size());

        finder = ipFinder();

        try {
            finder.setAddresses(Collections.singleton("[::1]:555..444"));

            fail();
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("[::1]:0..444"));

            fail();
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("[::1]:-8080..-80"));

            fail();
        }
        catch (GridSpiException e) {
            info("Caught expected exception: " + e);
        }

        finder.setAddresses(Collections.singleton("0:0:0:0:0:0:0:1"));

        assertEquals(1, finder.getRegisteredAddresses().size());

        finder.setAddresses(Collections.singleton("[0:0:0:0:0:0:0:1]"));

        assertEquals(1, finder.getRegisteredAddresses().size());

        finder.setAddresses(Collections.singleton("[0:0:0:0:0:0:0:1]:47509"));

        assertEquals(1, finder.getRegisteredAddresses().size());

        finder.setAddresses(Collections.singleton("[::1]:47500..47509"));

        assertEquals("Registered addresses: " + finder.getRegisteredAddresses().toString(),
            10, finder.getRegisteredAddresses().size());
    }
}
