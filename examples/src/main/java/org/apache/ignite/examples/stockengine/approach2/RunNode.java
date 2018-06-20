package org.apache.ignite.examples.stockengine.approach2;

import org.apache.ignite.Ignition;

public class RunNode {
    public static void main(String[] args) {
        Ignition.start("ignite_replication.xml");
    }
}
