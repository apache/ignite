/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

import java.util.Arrays;
import java.util.List;
import org.h2.jaqu.Table.JQTable;

/**
 * This class inherits all its fields from a parent class which has annotated
 * columns.  The JQTable annotation of the parent class is ignored and only
 * the JQTable annotation of this class matters.
 * However, this table inherits JQColumns from its super class.
 */
@JQTable(inheritColumns = true, annotationsOnly = false)
public class ProductInheritedAnnotation extends ProductMixedAnnotation {

    public ProductInheritedAnnotation() {
        // public constructor
    }

    private ProductInheritedAnnotation(int productId, String productName,
            String category, double unitPrice, int unitsInStock,
            String mappedField) {
        super(productId, productName, category, unitPrice, unitsInStock,
                mappedField);
    }

    private static ProductInheritedAnnotation create(int productId,
            String productName, String category, double unitPrice,
            int unitsInStock, String mappedField) {
        return new ProductInheritedAnnotation(productId, productName, category,
                unitPrice, unitsInStock, mappedField);
    }

    public static List<ProductInheritedAnnotation> getData() {
        String mappedField = "mapped";
        ProductInheritedAnnotation[] list = {
                create(1, "Chai", "Beverages", 18, 39, mappedField),
                create(2, "Chang", "Beverages", 19.0, 17, mappedField),
                create(3, "Aniseed Syrup", "Condiments", 10.0, 13, mappedField),
                create(4, "Chef Anton's Cajun Seasoning", "Condiments", 22.0, 53, mappedField),
                create(5, "Chef Anton's Gumbo Mix", "Condiments", 21.3500, 0, mappedField),
                create(6, "Grandma's Boysenberry Spread", "Condiments", 25.0, 120, mappedField),
                create(7, "Uncle Bob's Organic Dried Pears", "Produce", 30.0, 15, mappedField),
                create(8, "Northwoods Cranberry Sauce", "Condiments", 40.0, 6, mappedField),
                create(9, "Mishi Kobe Niku", "Meat/Poultry", 97.0, 29, mappedField),
                create(10, "Ikura", "Seafood", 31.0, 31, mappedField), };
        return Arrays.asList(list);
    }

}
