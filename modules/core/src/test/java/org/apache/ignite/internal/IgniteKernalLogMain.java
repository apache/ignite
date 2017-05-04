package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Created by gridgain on 24.04.2017.
 */
public class IgniteKernalLogMain {
    public static String prefix = "S.INCLUDE_SENSITIVE=";

    static void println(String msg) {
        System.out.println(msg);
    }

    public static void main(String... args) {

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConnectorConfiguration(null);

        println("Starting grid.");

        try (Ignite g = G.start(cfg)) {
            assert g != null;
            println(prefix + S.INCLUDE_SENSITIVE);
        }
    }

}
