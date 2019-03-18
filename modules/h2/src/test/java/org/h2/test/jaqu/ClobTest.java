/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

import static org.h2.jaqu.Define.primaryKey;
import static org.h2.jaqu.Define.tableName;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import org.h2.jaqu.Db;
import org.h2.jaqu.Table;
import org.h2.test.TestBase;

/**
 * Tests if converting a CLOB to a String works.
 */
public class ClobTest extends TestBase {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new ClobTest().test();
    }

    @Override
    public void test() throws Exception {
        String create = "CREATE TABLE CLOB_TEST(ID INT PRIMARY KEY, WORDS {0})";
        Db db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.executeUpdate(MessageFormat.format(create, "VARCHAR(255)"));
        db.insertAll(StringRecord.getList());
        testSimpleUpdate(db, "VARCHAR fail");
        db.close();

        db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.executeUpdate(MessageFormat.format(create, "TEXT"));
        db.insertAll(StringRecord.getList());
        testSimpleUpdate(db, "CLOB fail because of single quote artifacts");
        db.close();
    }

    private void testSimpleUpdate(Db db, String failureMsg) {
        String newWords = "I changed the words";
        StringRecord r = new StringRecord();
        StringRecord originalRecord = db.from(r).where(r.id).is(2).selectFirst();
        String oldWords = originalRecord.words;
        originalRecord.words = newWords;
        db.update(originalRecord);

        StringRecord r2 = new StringRecord();
        StringRecord revisedRecord = db.from(r2).where(r2.id).is(2).selectFirst();
        assertEquals(failureMsg, newWords, revisedRecord.words);

        // undo update
        originalRecord.words = oldWords;
        db.update(originalRecord);
    }

    /**
     * A simple class used in this test.
     */
    public static class StringRecord implements Table {

        public Integer id;
        public String words;

        public StringRecord() {
            // public constructor
        }

        private StringRecord(int id, String words) {
            this.id = id;
            this.words = words;
        }

        @Override
        public void define() {
            tableName("CLOB_TEST");
            primaryKey(id);
        }

        private static StringRecord create(int id, String words) {
            return new StringRecord(id, words);
        }

        public static List<StringRecord> getList() {
            StringRecord[] list = {
                    create(1, "Once upon a midnight dreary, while I pondered weak and weary,"),
                    create(2, "Over many a quaint and curious volume of forgotten lore,"),
                    create(3, "While I nodded, nearly napping, suddenly there came a tapping,"),
                    create(4, "As of some one gently rapping, rapping at my chamber door."),
                    create(5, "`'Tis some visitor,' I muttered, `tapping at my chamber door -"),
                    create(6, "Only this, and nothing more.'") };

            return Arrays.asList(list);
        }

        @Override
        public String toString() {
            return id + ": " + words;
        }
    }
}

