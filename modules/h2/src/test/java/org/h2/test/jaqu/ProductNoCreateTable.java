/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

import java.util.Arrays;
import java.util.List;
import org.h2.jaqu.Table.JQColumn;
import org.h2.jaqu.Table.JQTable;

/**
 * A table containing product data.
 */
@JQTable(createIfRequired = false)
public class ProductNoCreateTable {

    @JQColumn(name = "id")
    private Integer productId;

    @JQColumn(name = "name")
    private String productName;

    public ProductNoCreateTable() {
        // public constructor
    }

    private ProductNoCreateTable(int productId, String productName) {
        this.productId = productId;
        this.productName = productName;
    }

    private static ProductNoCreateTable create(int productId, String productName) {
        return new ProductNoCreateTable(productId, productName);
    }

    public static List<ProductNoCreateTable> getList() {
        ProductNoCreateTable[] list = { create(1, "Chai"), create(2, "Chang") };
        return Arrays.asList(list);
    }

}
