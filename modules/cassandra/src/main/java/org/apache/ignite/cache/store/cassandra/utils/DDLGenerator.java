package org.apache.ignite.cache.store.cassandra.utils;

import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;

import java.io.File;

public class DDLGenerator {
    public static void main(String[] args) {
        if (args == null || args.length == 0)
            return;

        for (String arg : args) {
            File file = new File(arg);
            if (!file.isFile()) {
                System.out.println("-------------------------------------------------------------");
                System.out.println("Incorrect file specified: " + arg);
                System.out.println("-------------------------------------------------------------");
                continue;
            }

            try {
                KeyValuePersistenceSettings settings = new KeyValuePersistenceSettings(file);
                System.out.println("-------------------------------------------------------------");
                System.out.println("DDL for keyspace/table from file: " + arg);
                System.out.println("-------------------------------------------------------------");
                System.out.println();
                System.out.println(settings.getKeyspaceDDLStatement());
                System.out.println();
                System.out.println(settings.getTableDDLStatement());
                System.out.println();
            }
            catch (Throwable e) {
                System.out.println("-------------------------------------------------------------");
                System.out.println("Incorrect file specified: " + arg);
                System.out.println("-------------------------------------------------------------");
                e.printStackTrace();
            }
        }
    }
}
