/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.jaqu.Db;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;

/**
 * Test annotation processing.
 */
public class AnnotationsTest extends TestBase {

    /**
     * This object represents a database (actually a connection to the
     * database).
     */
    private Db db;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws SQLException {
        new AnnotationsTest().test();
    }

    @Override
    public void test() throws SQLException {
        db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.insertAll(Product.getList());
        db.insertAll(ProductAnnotationOnly.getList());
        db.insertAll(ProductMixedAnnotation.getList());
        testIndexCreation();
        testProductAnnotationOnly();
        testProductMixedAnnotation();
        testTrimStringAnnotation();
        testCreateTableIfRequiredAnnotation();
        testColumnInheritanceAnnotation();
        db.close();
    }

    private void testIndexCreation() throws SQLException {
        // test indexes are created, and columns are in the right order
        DatabaseMetaData meta = db.getConnection().getMetaData();
        ResultSet rs = meta.getIndexInfo(null, "PUBLIC",
                "ANNOTATED" + "PRODUCT", false, true);
        assertTrue(rs.next());
        assertStartsWith(rs.getString("INDEX_NAME"), "PRIMARY_KEY");
        assertTrue(rs.next());
        assertStartsWith(rs.getString("INDEX_NAME"), "ANNOTATED" + "PRODUCT_");
        assertStartsWith(rs.getString("COLUMN_NAME"), "NAME");
        assertTrue(rs.next());
        assertStartsWith(rs.getString("INDEX_NAME"), "ANNOTATED" + "PRODUCT_");
        assertStartsWith(rs.getString("COLUMN_NAME"), "CAT");
        assertFalse(rs.next());
    }

    private void testProductAnnotationOnly() {
        ProductAnnotationOnly p = new ProductAnnotationOnly();
        assertEquals(10, db.from(p).selectCount());

        // test JQColumn.name="cat"
        assertEquals(2, db.from(p).where(p.category).is("Beverages").selectCount());

        // test JQTable.annotationsOnly=true
        // public String unmappedField is ignored by JaQu
        assertEquals(0, db.from(p).where(p.unmappedField).is("unmapped").selectCount());

        // test JQColumn.autoIncrement=true
        // 10 objects, 10 autoIncremented unique values
        assertEquals(10, db.from(p).selectDistinct(p.autoIncrement).size());

        // test JQTable.primaryKey=id
        try {
            db.insertAll(ProductAnnotationOnly.getList());
        } catch (RuntimeException r) {
            SQLException s = (SQLException) r.getCause();
            assertEquals(ErrorCode.DUPLICATE_KEY_1, s.getErrorCode());
        }
    }

    private void testProductMixedAnnotation() {
        ProductMixedAnnotation p = new ProductMixedAnnotation();

        // test JQColumn.name="cat"
        assertEquals(2, db.from(p).where(p.category).is("Beverages").selectCount());

        // test JQTable.annotationsOnly=false
        // public String mappedField is reflectively mapped by JaQu
        assertEquals(10, db.from(p).where(p.mappedField).is("mapped").selectCount());

        // test JQColumn.primaryKey=true
        try {
            db.insertAll(ProductMixedAnnotation.getList());
        } catch (RuntimeException r) {
            SQLException s = (SQLException) r.getCause();
            assertEquals(ErrorCode.DUPLICATE_KEY_1, s.getErrorCode());
        }
    }

    private void testTrimStringAnnotation() {
        ProductAnnotationOnly p = new ProductAnnotationOnly();
        ProductAnnotationOnly prod = db.from(p).selectFirst();
        String oldValue = prod.category;
        String newValue = "01234567890123456789";
        // 2 chars exceeds field max
        prod.category = newValue;
        db.update(prod);

        ProductAnnotationOnly newProd = db.from(p)
            .where(p.productId)
            .is(prod.productId)
            .selectFirst();
        assertEquals(newValue.substring(0, 15), newProd.category);

        newProd.category = oldValue;
        db.update(newProd);
    }

    private void testColumnInheritanceAnnotation() {
        ProductInheritedAnnotation table = new ProductInheritedAnnotation();
        Db db = Db.open("jdbc:h2:mem:", "sa", "sa");
        List<ProductInheritedAnnotation> inserted = ProductInheritedAnnotation
                .getData();
        db.insertAll(inserted);

        List<ProductInheritedAnnotation> retrieved = db.from(table).select();

        for (int j = 0; j < retrieved.size(); j++) {
            ProductInheritedAnnotation i = inserted.get(j);
            ProductInheritedAnnotation r = retrieved.get(j);
            assertEquals(i.category, r.category);
            assertEquals(i.mappedField, r.mappedField);
            assertEquals(i.unitsInStock, r.unitsInStock);
            assertEquals(i.unitPrice, r.unitPrice);
            assertEquals(i.name(), r.name());
            assertEquals(i.id(), r.id());
        }
        db.close();
    }

    private void testCreateTableIfRequiredAnnotation() {
        // tests JQTable.createTableIfRequired=false
        Db noCreateDb = null;
        try {
            noCreateDb = Db.open("jdbc:h2:mem:", "sa", "sa");
            noCreateDb.insertAll(ProductNoCreateTable.getList());
            noCreateDb.close();
        } catch (RuntimeException r) {
            SQLException s = (SQLException) r.getCause();
            assertEquals(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, s.getErrorCode());
        }
        if (noCreateDb != null) {
            JdbcUtils.closeSilently(noCreateDb.getConnection());
        }
    }

}
