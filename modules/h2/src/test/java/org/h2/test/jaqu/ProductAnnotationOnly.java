/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

import java.util.Arrays;
import java.util.List;
import org.h2.jaqu.Table.JQColumn;
import org.h2.jaqu.Table.JQIndex;
import org.h2.jaqu.Table.JQTable;

/**
 * A table containing product data.
 */
@JQTable(name = "AnnotatedProduct", primaryKey = "id")
@JQIndex(standard = "name, cat")
public class ProductAnnotationOnly {

    @JQColumn(autoIncrement = true)
    public Integer autoIncrement;

    public String unmappedField;

    @JQColumn(name = "id")
    Integer productId;

    @JQColumn(name = "cat", maxLength = 15, trimString = true)
    String category;

    @JQColumn(name = "name")
    private String productName;

    @JQColumn
    private Double unitPrice;

    @JQColumn
    private Integer unitsInStock;

    public ProductAnnotationOnly() {
        // public constructor
    }

    private ProductAnnotationOnly(int productId, String productName,
            String category, double unitPrice, int unitsInStock,
            String unmappedField) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.unitPrice = unitPrice;
        this.unitsInStock = unitsInStock;
        this.unmappedField = unmappedField;
    }

    private static ProductAnnotationOnly create(int productId,
            String productName, String category, double unitPrice,
            int unitsInStock, String unmappedField) {
        return new ProductAnnotationOnly(productId, productName, category,
                unitPrice, unitsInStock, unmappedField);
    }

    public static List<ProductAnnotationOnly> getList() {
        String unmappedField = "unmapped";
        ProductAnnotationOnly[] list = {
                create(1, "Chai", "Beverages", 18, 39,
                        unmappedField),
                create(2, "Chang", "Beverages", 19.0, 17,
                        unmappedField),
                create(3, "Aniseed Syrup", "Condiments", 10.0, 13,
                        unmappedField),
                create(4, "Chef Anton's Cajun Seasoning", "Condiments", 22.0, 53,
                        unmappedField),
                create(5, "Chef Anton's Gumbo Mix", "Condiments", 21.3500, 0,
                        unmappedField),
                create(6, "Grandma's Boysenberry Spread", "Condiments", 25.0, 120,
                        unmappedField),
                create(7, "Uncle Bob's Organic Dried Pears", "Produce", 30.0, 15,
                        unmappedField),
                create(8, "Northwoods Cranberry Sauce", "Condiments", 40.0, 6,
                        unmappedField),
                create(9, "Mishi Kobe Niku", "Meat/Poultry", 97.0, 29,
                        unmappedField),
                create(10, "Ikura", "Seafood", 31.0, 31,
                        unmappedField), };
        return Arrays.asList(list);
    }

    @Override
    public String toString() {
        return productName + ": " + unitsInStock;
    }

}
