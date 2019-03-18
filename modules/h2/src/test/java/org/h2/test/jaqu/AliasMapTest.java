/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

import java.util.List;
import org.h2.jaqu.Db;
import org.h2.test.TestBase;

/**
 * Tests that columns (p.unitsInStock) are not compared by value with the value
 * (9), but by reference (using an identity hash map).
 * See http://code.google.com/p/h2database/issues/detail?id=119
 *
 * @author d moebius at scoop slash gmbh dot de
 */
public class AliasMapTest extends TestBase {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new AliasMapTest().test();
    }

    @Override
    public void test() throws Exception {
        Db db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.insertAll(Product.getList());

        Product p = new Product();
        List<Product> products = db
            .from(p)
            .where(p.unitsInStock).is(9)
            .orderBy(p.productId).select();

        assertEquals("[]", products.toString());

        db.close();
    }
}

