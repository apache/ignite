package org.apache.ignite.internal;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Tests change coordinator event logging
 */
public class GridDiscoveryManagerChangeCoordinatorTest extends GridCommonAbstractTest {
    /** */
    private GridStringLogger strLog;

    /** */
    private final String strPtrn = "Coordinator changed \\[prev=TcpDiscoveryNode \\[id=%s.*cur=TcpDiscoveryNode \\[id=%s";

    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();
        stopAllGrids();
        strLog = new GridStringLogger();
    }

    @Override
    protected void afterTest() throws Exception {
        super.afterTest();
        strLog = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        cfg.setDiscoverySpi(discoSpi);

        if("client".equals(igniteInstanceName))
            cfg.setClientMode(true);
        else if("daemon".equals(igniteInstanceName))
            cfg.setDaemon(true);

        if (strLog != null)
            cfg.setGridLogger(strLog);

        return cfg;
    }

    /**
     * Tests change coordinator event logging
     * @throws Exception If failed
     */
    public void testChangeCoordinatorLogging() throws Exception {

        //Стартуем два СУ
        IgniteEx srv1 = (IgniteEx)startGrid("server1");
        IgniteEx srv2 = (IgniteEx)startGrid("server2");

        //Активируем грид
        srv1.cluster().active();

        //Координатор = server1
        UUID crdUUID = ((TcpDiscoverySpi) srv1.context().config().getDiscoverySpi()).getCoordinator();
        assertEquals(srv1.localNode().id(), crdUUID);

        //Вводим клиента, демона и третий серверный узел, который не в бейзлайне
        IgniteEx client = (IgniteEx)startGrid("client");
        IgniteEx daemon = (IgniteEx)startGrid("daemon");
        IgniteEx srv3 = (IgniteEx)startGrid("server3");

        UUID srv1ClusterNode = srv1.localNode().id();
        UUID srv2ClusterNode = srv2.localNode().id();

        //Выводим server1
        stopGrid("server1");
        //Координатор сменился server1 -> server2, в логах есть запись об этом
        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();
        assertEquals(srv2.localNode().id(), crdUUID);
        Pattern ptrn = Pattern.compile(String.format(strPtrn, srv1ClusterNode, srv2ClusterNode));
        assertTrue(ptrn.matcher(strLog.toString()).find());
        strLog.reset();

        //Вводим server1
        srv1 = (IgniteEx)startGrid("server1");
        //Координатор все еще server2, записей о смене координатора server2 -> server1 нет
        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();
        assertEquals(srv2.localNode().id(), crdUUID);
        ptrn = Pattern.compile(String.format(strPtrn, srv2ClusterNode, srv1ClusterNode));
        assertFalse(ptrn.matcher(strLog.toString()).find());
        strLog.reset();

        //Выводим server2
        stopGrid("server2");
        //Координатор сменился server2 -> daemon, в логах есть запись об этом
        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();
        assertEquals(daemon.localNode().id(), crdUUID);
        ptrn = Pattern.compile(String.format(strPtrn, srv2ClusterNode, daemon.localNode().id()));
        assertTrue(ptrn.matcher(strLog.toString()).find());
        strLog.reset();

        //Выводим client
        stopGrid("client");
        //Координатор все еще daemon, записей о смене координатора в логах нет
        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();
        assertEquals(daemon.localNode().id(), crdUUID);
        assertFalse(strLog.toString().contains("Coordinator changed"));
        strLog.reset();

        //Выводим server3
        stopGrid("server3");
        //Координатор все еще daemon, записей о смене координатора в логах нет
        crdUUID = ((TcpDiscoverySpi) srv1.context().config().getDiscoverySpi()).getCoordinator();
        assertEquals(daemon.localNode().id(), crdUUID);
        assertFalse(strLog.toString().contains("Coordinator changed"));
    }
}
