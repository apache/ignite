/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

import java.util.Arrays;
import java.util.List;

/**
 * A table containing customer data.
 */
public class Customer {

    public String customerId;
    public String region;

    public Customer() {
        // public constructor
    }

    public Customer(String customerId, String region) {
        this.customerId = customerId;
        this.region = region;
    }

    @Override
    public String toString() {
        return customerId;
    }

    public static List<Customer> getList() {
        Customer[] list = {
                new Customer("ALFKI", "WA"),
                new Customer("ANATR", "WA"),
                new Customer("ANTON", "CA") };
        return Arrays.asList(list);
    }

}
