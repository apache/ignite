/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

import static org.h2.jaqu.Define.index;
import static org.h2.jaqu.Define.maxLength;
import static org.h2.jaqu.Define.primaryKey;
import static org.h2.jaqu.Define.tableName;
import java.util.Arrays;
import java.util.List;
import org.h2.jaqu.Table;

/**
 * A table containing product data.
 */
public class Product implements Table {

    public Integer productId;
    public String productName;
    public String category;
    public Double unitPrice;
    public Integer unitsInStock;

    public Product() {
        // public constructor
    }

    private Product(int productId, String productName,
            String category, double unitPrice, int unitsInStock) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.unitPrice = unitPrice;
        this.unitsInStock = unitsInStock;
    }

    @Override
    public void define() {
        tableName("Product");
        primaryKey(productId);
        maxLength(category, 255);
        index(productName, category);
    }

    private static Product create(int productId, String productName,
            String category, double unitPrice, int unitsInStock) {
        return new Product(productId, productName, category,
            unitPrice, unitsInStock);
    }

    public static List<Product> getList() {
        Product[] list = {
                create(1, "Chai", "Beverages", 18, 39),
                create(2, "Chang", "Beverages", 19.0, 17),
                create(3, "Aniseed Syrup", "Condiments", 10.0, 13),
                create(4, "Chef Anton's Cajun Seasoning", "Condiments", 22.0, 53),
                create(5, "Chef Anton's Gumbo Mix", "Condiments", 21.3500, 0),
                create(6, "Grandma's Boysenberry Spread", "Condiments", 25.0, 120),
                create(7, "Uncle Bob's Organic Dried Pears", "Produce", 30.0, 15),
                create(8, "Northwoods Cranberry Sauce", "Condiments", 40.0, 6),
                create(9, "Mishi Kobe Niku", "Meat/Poultry", 97.0, 29),
                create(10, "Ikura", "Seafood", 31.0, 31),
        };

        return Arrays.asList(list);
    }

    @Override
    public String toString() {
        return productName + ": " + unitsInStock;
    }

}
