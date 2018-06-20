package org.apache.ignite.examples.stockengine.approach2;

import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.OptionType;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.Side;
import org.apache.ignite.examples.stockengine.domain.State;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;

public class RunApproach2 {
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("ignite_replication.xml")) {
            ignite.cluster().active(true);

            // Deploy services only on server nodes.
            IgniteServices svcs = ignite.services(ignite.cluster().forServers());



            // Deploy cluster singleton.
            svcs.deployKeyAffinitySingleton("engine", new DurableEngine(), "users", 1);



            IgniteCache<Object, Object> order = ignite.cache("orders");


            for (int i = 0; i < 100; i++)
                order.put((long) i, new Order(i, 0, OptionType.SIMPLE, Side.BUY, new Instrument("EUR/USE"),
                        System.currentTimeMillis() + 1000, 1.0000, i % 2 == 0 ? State.OPEN : State.CLOSED));



            System.err.println("FIRST STAGE DONE!");


            for (int i = 100; i < 150; i++) {
                order.put((long) i, new Order(i, 0, OptionType.SIMPLE, Side.BUY, new Instrument("EUR/USE"),
                        System.currentTimeMillis() + 1000, 1.0000, i % 2 == 0 ? State.OPEN : State.CLOSED));
            }

            for (int i = 150; i < 200; i++) {
                order.put((long) i, new Order(i, 0, OptionType.SIMPLE, Side.BUY, new Instrument("EUR/USE"),
                        System.currentTimeMillis() + 1000, 1.0000, i % 2 == 0 ? State.OPEN : State.CLOSED));
            }

            for (int i = 0; i < 110; i++)
                order.remove((long) i);

            Printer printer = svcs.service("engine");

            printer.printState();
        }
    }
}
