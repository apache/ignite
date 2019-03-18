/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

import static java.sql.Date.valueOf;
import org.h2.jaqu.Db;
import org.h2.test.TestBase;

/**
 * Tests the Db.update() function.
 *
 * @author dmoebius at scoop slash gmbh dot de
 */
public class UpdateTest extends TestBase {

    private Db db;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new UpdateTest().test();
    }

    @Override
    public void test() throws Exception {
        db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.insertAll(Product.getList());
        db.insertAll(Customer.getList());
        db.insertAll(Order.getList());

        testSimpleUpdate();
        testSimpleUpdateWithCombinedPrimaryKey();
        testSimpleMerge();
        testSimpleMergeWithCombinedPrimaryKey();
        testSetColumns();

        db.close();
    }

    private void testSimpleUpdate() {
        Product p = new Product();
        Product pChang = db.from(p).where(p.productName).is("Chang")
                .selectFirst();
        // update unitPrice from 19.0 to 19.5
        pChang.unitPrice = 19.5;
        // update unitsInStock from 17 to 16
        pChang.unitsInStock = 16;
        db.update(pChang);

        Product p2 = new Product();
        Product pChang2 = db.from(p2).where(p2.productName).is("Chang")
                .selectFirst();
        assertEquals((Double) 19.5, pChang2.unitPrice);
        assertEquals(16, pChang2.unitsInStock.intValue());

        // undo update
        pChang.unitPrice = 19.0;
        pChang.unitsInStock = 17;
        db.update(pChang);
    }

    private void testSimpleUpdateWithCombinedPrimaryKey() {
        Order o = new Order();
        Order ourOrder = db.from(o).where(o.orderDate)
                .is(valueOf("2007-01-02")).selectFirst();
        ourOrder.orderDate = valueOf("2007-01-03");
        db.update(ourOrder);

        Order ourUpdatedOrder = db.from(o).where(o.orderDate)
                .is(valueOf("2007-01-03")).selectFirst();
        assertTrue("updated order not found", ourUpdatedOrder != null);

        // undo update
        ourOrder.orderDate = valueOf("2007-01-02");
        db.update(ourOrder);
    }

    private void testSimpleMerge() {
        Product p = new Product();
        Product pChang = db.from(p).where(p.productName).is("Chang")
                .selectFirst();
        // update unitPrice from 19.0 to 19.5
        pChang.unitPrice = 19.5;
        // update unitsInStock from 17 to 16
        pChang.unitsInStock = 16;
        db.merge(pChang);

        Product p2 = new Product();
        Product pChang2 = db.from(p2).where(p2.productName).is("Chang")
                .selectFirst();
        assertEquals((Double) 19.5, pChang2.unitPrice);
        assertEquals(16, pChang2.unitsInStock.intValue());

        // undo update
        pChang.unitPrice = 19.0;
        pChang.unitsInStock = 17;
        db.merge(pChang);
    }

    private void testSimpleMergeWithCombinedPrimaryKey() {
        Order o = new Order();
        Order ourOrder = db.from(o).where(o.orderDate)
                .is(valueOf("2007-01-02")).selectFirst();
        ourOrder.orderDate = valueOf("2007-01-03");
        db.merge(ourOrder);

        Order ourUpdatedOrder = db.from(o).where(o.orderDate)
                .is(valueOf("2007-01-03")).selectFirst();
        assertTrue("updated order not found", ourUpdatedOrder != null);

        // undo update
        ourOrder.orderDate = valueOf("2007-01-02");
        db.merge(ourOrder);
    }

    private void testSetColumns() {
        Product p = new Product();
        Product original = db.from(p).where(p.productId).is(1).selectFirst();

        // update  string and double columns
        db.from(p)
            .set(p.productName).to("updated")
            .increment(p.unitPrice).by(3.14)
            .increment(p.unitsInStock).by(2)
            .where(p.productId)
            .is(1).
            update();

        // confirm the data was properly updated
        Product revised = db.from(p).where(p.productId).is(1).selectFirst();
        assertEquals("updated", revised.productName);
        assertEquals((Double) (original.unitPrice + 3.14), revised.unitPrice);
        assertEquals(original.unitsInStock + 2, revised.unitsInStock.intValue());

        // restore the data
        db.from(p)
            .set(p.productName).to(original.productName)
            .set(p.unitPrice).to(original.unitPrice)
            .increment(p.unitsInStock).by(-2)
            .where(p.productId).is(1).update();

        // confirm the data was properly restored
        Product restored = db.from(p).
                where(p.productId).is(1).selectFirst();
        assertEquals(original.productName, restored.productName);
        assertEquals(original.unitPrice, restored.unitPrice);
        assertEquals(original.unitsInStock, restored.unitsInStock);

        double unitPriceOld = db.from(p).
                where(p.productId).is(1).selectFirst().unitPrice;
        // double the unit price
        db.from(p).increment(p.unitPrice).by(p.unitPrice).where(p.productId)
                .is(1).update();
        double unitPriceNew = db.from(p).
                where(p.productId).is(1).selectFirst().unitPrice;
        assertEquals(unitPriceOld * 2, unitPriceNew);

    }

}
