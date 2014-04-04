/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.json;

import org.apache.commons.lang.time.*;
import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.common.*;
import java.util.*;

/**
 * Test for JSON deserializer.
 */
@SuppressWarnings({"FloatingPointEquality", "SizeReplaceableByIsEmpty", "FieldCanBeLocal"})
@GridCommonTest(group = "Utils")
public class GridJsonDeserializerSelfTest extends GridCommonAbstractTest {
    /**  */
    private String jsonA = "{valBooleanObj:true,valDoubleObj:9876.9876,valFloatObj:1234.123,valIntegerObj:1234," +
        "valLongObj:9876,valString:'String',valBoolean:false,valDouble:5432.5432,valFloat:5678.567," +
        "valInt:5678,valLong:5432,@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild'}";

    /**  */
    private String jsonB = "{valBooleanObj:true,valDoubleObj:9876.9876,valFloatObj:1234.123,valIntegerObj:1234," +
        "valLongObj:9876,valString:'String',valBoolean:false,valDouble:5432.5432,valFloat:5678.567,valInt:5678," +
        "valLong:5432}";

    /**  */
    private String jsonC = "{@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild'}";

    /**  */
    private String jsonD = "{}";

    /**  */
    private String jsonE = "{valChildArray:[{valString:'TestChild0'},{valString:'TestChild1'}]," +
        "valStringArray:['String0','String1']," +
        "@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestParent'}";

    /**  */
    private String jsonF = "{valChildArray:[{valString:'TestChild0'},{valString:'TestChild1'}]," +
        "valStringArray:['String0','String1']}";

    /**  */
    private String jsonG = "{valMap:{stringVal:'String1',intVal:100}," +
        "valLinkedHashMap:{stringVal:'String2',doubleVal:1234.1234}," +
        "@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestParent'}";

    /**  */
    private String jsonH = "{valMap:{stringVal:'String1',intVal:100}," +
        "valLinkedHashMap:{stringVal:'String2',doubleVal:1234.1234}}";

    /**  */
    private String jsonI = "{valChildMap:{child0:{valString:'TestChild0'},child1:{valString:'TestChild1'}," +
        "@class:'java.util.LinkedHashMap'}," +
        "@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestParent'}";

    /**  */
    private String jsonJ = "{valChildMap:{child0:{valString:'TestChild0'},child1:{valString:'TestChild1'}," +
        "@class:'java.util.LinkedHashMap'," +
        "@elem:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild'}}";

    /**  */
    private String jsonK = "{valCollection:['String0','String1'],valLinkedList:[0,1,1,-1]," +
        "valLinkedHashSet:[0,1,1,-1],@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestParent'}";

    /**  */
    private String jsonL = "{valCollection:['String0','String1']," +
        "valLinkedList:[0,1,1,-1,{@class:'java.util.LinkedList'}]," +
        "valLinkedHashSet:[0,1,1,-1,{@class:'java.util.LinkedHashSet'}]}";

    /**  */
    private String jsonM = "{valChildList:[{valString:'TestChild0'},{valString:'TestChild1'}," +
        "{@class:'java.util.LinkedList'}]," +
        "valChildSet:[{valString:'TestChild2'}]," +
        "@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestParent'}";

    /**  */
    private String jsonN = "{valChildList:[{valString:'TestChild0'},{valString:'TestChild1'}," +
        "{@class:'java.util.LinkedList'," +
        "@elem:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild'}]," +
        "valChildSet:[{valString:'TestChild2'},{@class:'java.util.HashSet'," +
        "@elem:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild'}]}";

    /**  */
    private String jsonO = "{valDate:'20110429230930',valCalendar:'2011-04-29 23:09:30'," +
        "@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild'}";

    /**  */
    private String jsonP = "{valBooleanObj:true,valDoubleObj:9876.9876,valFloatObj:1234.123,valIntegerObj:1234," +
        "valLongObj:9876,valString:'String',valBoolean:false,valDouble:5432.5432,valFloat:5678.567," +
        "valDate:'20110429230930',valCalendar:'2011-04-29 23:09:30',valInt:5678,valLong:5432," +
        "@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild'}";

    /**  */
    private String jsonQ = "{valBooleanObj:true,valDoubleObj:9876.9876,valFloatObj:1234.123,valIntegerObj:1234," +
        "valLongObj:9876,valString:'String',valBoolean:false,valDouble:5432.5432,valFloat:5678.567,valInt:5678," +
        "valLong:5432,valDate:'20110429230930',valCalendar:'2011-04-29 23:09:30',valInt:5678}";

    /**  */
    private String jsonR = "{valChildArray:[{valString:'TestChild0'},{valString:'TestChild1'}]," +
        "valStringArray:['String0','String1']," +
        "@class:'org.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestParent'}";

    /**  */
    private String jsonS = "{valChildArray:[{valString:'TestChild0'},{valString:'TestChild1'}," +
        "{@class='[Lorg.gridgain.grid.util.json.GridJsonDeserializerSelfTest$TestChild;'}]," +
        "valStringArray:['String0','String1']}";

    /**
     * @throws Exception If failed.
     */
    public void testA() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonA);

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assertEquals(9876.987, child.valDoubleObj);
        assertEquals(5432.543, child.valDouble);
        assert child.valFloatObj == 1234.123F;
        assert child.valFloat == 5678.567F;
        assert child.valLongObj == 9876L;
        assert child.valLong == 5432L;
        assert child.valIntegerObj == 1234;
        assert child.valInt == 5678;
        assert child.valBooleanObj;
        assert !child.valBoolean;
        assert "String".equals(child.valString);

        log().info(child.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testB() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonB);

        assert obj != null;
        assert obj instanceof HashMap;

        Map map = (Map)obj;

        assert map.size() == 11;

        assertEquals(9876.987, map.get("valDoubleObj"));
        assertEquals(5432.543, map.get("valDouble"));
        assert map.get("valIntegerObj").equals(1234);
        assert map.get("valInt").equals(5678);
        assert map.get("valBooleanObj").equals(true);
        assert map.get("valBoolean").equals(false);
        assert "String".equals(map.get("valString"));

        assert map.get("valLongObj") instanceof Integer;
        assert map.get("valLongObj").equals(9876);
        assert map.get("valLong") instanceof Integer;
        assert map.get("valLong").equals(5432);

        assert map.get("valFloatObj") instanceof Double;
        assert map.get("valFloatObj").equals(1234.123);
        assert map.get("valFloat") instanceof Double;
        assert map.get("valFloat").equals(5678.567);

        log().info(map.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testC() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonC);

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert child.valDoubleObj == null;
        assert child.valDouble == 0;
        assert child.valFloatObj == null;
        assert child.valFloat == 0;
        assert child.valLongObj == null;
        assert child.valLong == 0;
        assert child.valIntegerObj == null;
        assert child.valInt == 0;
        assert child.valBooleanObj == null;
        assert !child.valBoolean;
        assert child.valString == null;

        log().info(child.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testD() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonD);

        assert obj != null;
        assert obj instanceof HashMap;

        Map map = (Map)obj;

        assert map.size() == 0;

        log().info(map.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testE() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonE);

        assert obj != null;
        assert obj instanceof TestParent;

        TestParent parent = (TestParent)obj;

        assert parent.valStringArray != null;
        assert parent.valStringArray.length == 2;

        assert "String0".equals(parent.valStringArray[0]);
        assert "String1".equals(parent.valStringArray[1]);

        assert parent.valChildArray != null;
        assert parent.valChildArray.length == 2;

        obj = parent.valChildArray[0];

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert "TestChild0".equals(child.valString);

        obj = parent.valChildArray[1];

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild1".equals(child.valString);

        assert parent.valMap == null;
        assert parent.valChildMap == null;
        assert parent.valLinkedHashMap == null;
        assert parent.valCollection == null;
        assert parent.valChildList == null;
        assert parent.valLinkedList == null;
        assert parent.valChildSet == null;
        assert parent.valLinkedHashSet == null;

        log().info(parent.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testF() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonF);

        assert obj != null;
        assert obj instanceof HashMap;

        Map map = (Map)obj;

        assert map.size() == 2;

        obj = map.get("valStringArray");

        assert obj != null;
        assert obj instanceof ArrayList;

        List list = (List)obj;

        assert list.size() == 2;

        assert "String0".equals(list.get(0));
        assert "String1".equals(list.get(1));

        obj = map.get("valChildArray");

        assert obj != null;
        assert obj instanceof ArrayList;

        list = (List)obj;

        assert list.size() == 2;

        obj = list.get(0);

        assert obj instanceof HashMap;

        Map childMap = (Map)obj;

        assert childMap.size() == 1;

        assert childMap.get("valString") != null;
        assert "TestChild0".equals(childMap.get("valString"));

        obj = list.get(1);

        assert obj instanceof HashMap;

        childMap = (Map)obj;

        assert childMap.size() == 1;

        assert childMap.get("valString") != null;
        assert "TestChild1".equals(childMap.get("valString"));

        log().info(map.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testG() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonG);

        assert obj != null;
        assert obj instanceof TestParent;

        TestParent parent = (TestParent)obj;

        assert parent.valMap != null;
        assert parent.valMap instanceof HashMap;
        assert parent.valMap.size() == 2;

        assert parent.valMap.get("stringVal") != null;
        assert "String1".equals(parent.valMap.get("stringVal"));
        assert parent.valMap.get("intVal") != null;
        assert parent.valMap.get("intVal").equals(100);

        assert parent.valLinkedHashMap != null;
        assert parent.valLinkedHashMap instanceof LinkedHashMap;
        assert parent.valLinkedHashMap.size() == 2;

        assert parent.valLinkedHashMap.get("stringVal") != null;
        assert "String2".equals(parent.valLinkedHashMap.get("stringVal"));
        assert parent.valLinkedHashMap.get("doubleVal") != null;
        assert parent.valLinkedHashMap.get("doubleVal").equals(1234.1234);

        assert parent.valStringArray == null;
        assert parent.valChildArray == null;
        assert parent.valChildMap == null;
        assert parent.valCollection == null;
        assert parent.valChildList == null;
        assert parent.valLinkedList == null;
        assert parent.valChildSet == null;
        assert parent.valLinkedHashSet == null;

        log().info(parent.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testH() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonH);

        assert obj != null;
        assert obj instanceof HashMap;

        Map map = (Map)obj;

        assert map.size() == 2;

        obj = map.get("valMap");

        assert obj != null;
        assert obj instanceof HashMap;

        Map childMap = (Map)obj;

        assert childMap.size() == 2;

        assert childMap.get("stringVal") != null;
        assert "String1".equals(childMap.get("stringVal"));
        assert childMap.get("intVal") != null;
        assert childMap.get("intVal").equals(100);

        obj = map.get("valLinkedHashMap");

        assert obj != null;
        assert obj instanceof HashMap;

        childMap = (Map)obj;

        assert childMap.size() == 2;

        assert childMap.get("stringVal") != null;
        assert "String2".equals(childMap.get("stringVal"));
        assert childMap.get("doubleVal") != null;
        assert childMap.get("doubleVal").equals(1234.1234);

        log().info(map.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testI() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonI);

        assert obj != null;
        assert obj instanceof TestParent;

        TestParent parent = (TestParent)obj;

        assert parent.valChildMap != null;
        assert parent.valChildMap instanceof LinkedHashMap;
        assert parent.valChildMap.size() == 2;

        obj = parent.valChildMap.get("child0");

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert "TestChild0".equals(child.valString);

        obj = parent.valChildMap.get("child1");

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild1".equals(child.valString);

        assert parent.valMap == null;
        assert parent.valLinkedHashMap == null;
        assert parent.valStringArray == null;
        assert parent.valChildArray == null;
        assert parent.valCollection == null;
        assert parent.valChildList == null;
        assert parent.valLinkedList == null;
        assert parent.valChildSet == null;
        assert parent.valLinkedHashSet == null;

        log().info(parent.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testJ() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonJ);

        assert obj != null;
        assert obj instanceof HashMap;

        Map map = (Map)obj;

        assert map.size() == 1;

        obj = map.get("valChildMap");

        assert obj != null;
        assert obj instanceof LinkedHashMap;

        Map childMap = (Map)obj;

        assert childMap.size() == 2;

        obj = childMap.get("child0");

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert "TestChild0".equals(child.valString);

        obj = childMap.get("child1");

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild1".equals(child.valString);

        log().info(map.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testK() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonK);

        assert obj != null;
        assert obj instanceof TestParent;

        TestParent parent = (TestParent)obj;

        assert parent.valCollection != null;
        assert parent.valCollection instanceof ArrayList;
        assert parent.valCollection.size() == 2;

        List list = (List)parent.valCollection;

        assert "String0".equals(list.get(0));
        assert "String1".equals(list.get(1));

        assert parent.valLinkedList != null;
        assert parent.valLinkedList instanceof LinkedList;
        assert parent.valLinkedList.size() == 4;

        assert (Integer)parent.valLinkedList.get(0) == 0;
        assert (Integer)parent.valLinkedList.get(1) == 1;
        assert (Integer)parent.valLinkedList.get(2) == 1;
        assert (Integer)parent.valLinkedList.get(3) == -1;

        assert parent.valLinkedHashSet != null;
        assert parent.valLinkedHashSet instanceof LinkedHashSet;
        assert parent.valLinkedHashSet.size() == 3;

        Iterator iter = parent.valLinkedHashSet.iterator();

        assert (Integer)iter.next() == 0;
        assert (Integer)iter.next() == 1;
        assert (Integer)iter.next() == -1;

        assert parent.valMap == null;
        assert parent.valChildMap == null;
        assert parent.valLinkedHashMap == null;
        assert parent.valStringArray == null;
        assert parent.valChildArray == null;
        assert parent.valChildList == null;
        assert parent.valChildSet == null;

        log().info(parent.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testL() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonL);

        assert obj != null;
        assert obj instanceof HashMap;

        Map map = (Map)obj;

        assert map.size() == 3;

        obj = map.get("valCollection");

        assert obj != null;
        assert obj instanceof ArrayList;

        List list = (List)obj;

        assert list.size() == 2;

        assert "String0".equals(list.get(0));
        assert "String1".equals(list.get(1));

        obj = map.get("valLinkedList");

        assert obj != null;
        assert obj instanceof LinkedList;

        list = (List)obj;

        assert list.size() == 4;

        assert (Integer)list.get(0) == 0;
        assert (Integer)list.get(1) == 1;
        assert (Integer)list.get(2) == 1;
        assert (Integer)list.get(3) == -1;

        obj = map.get("valLinkedHashSet");

        assert obj != null;
        assert obj instanceof LinkedHashSet;

        Collection set = (Collection)obj;

        assert set.size() == 3;

        Iterator iter = set.iterator();

        assert (Integer)iter.next() == 0;
        assert (Integer)iter.next() == 1;
        assert (Integer)iter.next() == -1;

        log().info(map.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testM() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonM);

        assert obj != null;
        assert obj instanceof TestParent;

        TestParent parent = (TestParent)obj;

        assert parent.valChildList != null;
        assert parent.valChildList instanceof LinkedList;
        assert parent.valChildList.size() == 2;

        obj = parent.valChildList.get(0);

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert "TestChild0".equals(child.valString);

        obj = parent.valChildList.get(1);

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild1".equals(child.valString);

        assert parent.valChildSet != null;
        assert parent.valChildSet instanceof HashSet;
        assert parent.valChildSet.size() == 1;

        obj = parent.valChildSet.iterator().next();

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild2".equals(child.valString);

        assert parent.valMap == null;
        assert parent.valChildMap == null;
        assert parent.valLinkedHashMap == null;
        assert parent.valStringArray == null;
        assert parent.valChildArray == null;
        assert parent.valCollection == null;
        assert parent.valLinkedList == null;
        assert parent.valLinkedHashSet == null;

        log().info(parent.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testN() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonN);

        assert obj != null;
        assert obj instanceof HashMap;

        Map map = (Map)obj;

        assert map.size() == 2;

        obj = map.get("valChildList");

        assert obj != null;
        assert obj instanceof LinkedList;

        List list = (List)obj;

        assert list.size() == 2;

        obj = list.get(0);

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert "TestChild0".equals(child.valString);

        obj = list.get(1);

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild1".equals(child.valString);

        obj = map.get("valChildSet");

        assert obj != null;
        assert obj instanceof HashSet;

        Collection set = (Collection)obj;

        assert set.size() == 1;

        obj = set.iterator().next();

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild2".equals(child.valString);

        log().info(map.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testO() throws Exception {
        Object obj = GridJsonDeserializer.deserialize(jsonO);

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert child.valDate != null;
        assert child.valDate.equals(DateUtils.parseDate("20110429230930", new String[] { "yyyyMMddHHmmss" }));

        assert child.valCalendar != null;
        assert child.valCalendar.getTime()
            .equals(DateUtils.parseDate("20110429230930", new String[] { "yyyyMMddHHmmss" }));

        log().info(child.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testP() throws Exception {
        TestChild child = new TestChild();

        GridJsonDeserializer.inject(child, jsonP);

        assertEquals(9876.987, child.valDoubleObj);
        assertEquals(5432.543, child.valDouble);
        assertEquals(1234.123F, child.valFloatObj);
        assertEquals(child.valFloat, 5678.567F);
        assertEquals((Long)9876L, child.valLongObj);
        assertEquals(5432L, child.valLong);
        assertEquals((Integer)1234, child.valIntegerObj);
        assertEquals(5678, child.valInt);
        assertTrue(child.valBooleanObj);
        assertTrue(!child.valBoolean);
        assertEquals("String", child.valString);
        assertNotNull(child.valDate);
        assertEquals(child.valDate, (DateUtils.parseDate("20110429230930", new String[] { "yyyyMMddHHmmss" })));
        assertNotNull(child.valCalendar);
        assertEquals(child.valCalendar.getTime(),
            (DateUtils.parseDate("20110429230930", new String[] { "yyyyMMddHHmmss" })));

        log().info(child.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testQ() throws Exception {
        TestChild child = new TestChild();

        GridJsonDeserializer.inject(child, jsonQ);

        assertEquals(9876.987, child.valDoubleObj);
        assertEquals(5432.543, child.valDouble);
        assert child.valFloatObj == 1234.123F;
        assert child.valFloat == 5678.567F;
        assert child.valLongObj == 9876L;
        assert child.valLong == 5432L;
        assert child.valIntegerObj == 1234;
        assert child.valInt == 5678;
        assert child.valBooleanObj;
        assert !child.valBoolean;
        assert "String".equals(child.valString);
        assert child.valDate != null;
        assert child.valDate.equals(DateUtils.parseDate("20110429230930", new String[] { "yyyyMMddHHmmss" }));
        assert child.valCalendar != null;
        assert child.valCalendar.getTime()
            .equals(DateUtils.parseDate("20110429230930", new String[] { "yyyyMMddHHmmss" }));

        log().info(child.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testR() throws Exception {
        TestParent parent = new TestParent();

        GridJsonDeserializer.inject(parent, jsonR);

        assert parent.valStringArray != null;
        assert parent.valStringArray.length == 2;

        assert "String0".equals(parent.valStringArray[0]);
        assert "String1".equals(parent.valStringArray[1]);

        assert parent.valChildArray != null;
        assert parent.valChildArray.length == 2;

        Object obj = parent.valChildArray[0];

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert "TestChild0".equals(child.valString);

        obj = parent.valChildArray[1];

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild1".equals(child.valString);

        assert parent.valMap == null;
        assert parent.valChildMap == null;
        assert parent.valLinkedHashMap == null;
        assert parent.valCollection == null;
        assert parent.valChildList == null;
        assert parent.valLinkedList == null;
        assert parent.valChildSet == null;
        assert parent.valLinkedHashSet == null;

        log().info(parent.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testS() throws Exception {
        TestParent parent = new TestParent();

        GridJsonDeserializer.inject(parent, jsonS);

        assert parent.valStringArray != null;
        assert parent.valStringArray.length == 2;

        assert "String0".equals(parent.valStringArray[0]);
        assert "String1".equals(parent.valStringArray[1]);

        assert parent.valChildArray != null;
        assert parent.valChildArray.length == 2;

        Object obj = parent.valChildArray[0];

        assert obj != null;
        assert obj instanceof TestChild;

        TestChild child = (TestChild)obj;

        assert "TestChild0".equals(child.valString);

        obj = parent.valChildArray[1];

        assert obj != null;
        assert obj instanceof TestChild;

        child = (TestChild)obj;

        assert "TestChild1".equals(child.valString);

        assert parent.valMap == null;
        assert parent.valChildMap == null;
        assert parent.valLinkedHashMap == null;
        assert parent.valCollection == null;
        assert parent.valChildList == null;
        assert parent.valLinkedList == null;
        assert parent.valChildSet == null;
        assert parent.valLinkedHashSet == null;

        log().info(parent.toString());
    }

    /**
     *
     */
    @SuppressWarnings( {"PublicInnerClass"})
    public static class TestParent {
        /**  */
        private String[] valStringArray;

        /**  */
        private TestChild[] valChildArray;

        /**  */
        private Map valMap;

        /**  */
        private Map<String, TestChild> valChildMap;

        /**  */
        private LinkedHashMap valLinkedHashMap;

        /**  */
        private Collection valCollection;

        /**  */
        private List<TestChild> valChildList;

        /**  */
        private LinkedList valLinkedList;

        /**  */
        private Set<TestChild> valChildSet;

        /**  */
        private LinkedHashSet valLinkedHashSet;

        /**
         * @return Value string array.
         */
        @SuppressWarnings("unused")
        public String[] getValStringArray() {
            return valStringArray;
        }

        /**
         * @param valStringArray Value string array.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValStringArray(String[] valStringArray) {
            this.valStringArray = valStringArray;
        }

        /**
         * @return Value child array.
         */
        @SuppressWarnings("unused")
        public TestChild[] getValChildArray() {
            return valChildArray;
        }

        /**
         * @param valChildArray Value child array.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValChildArray(TestChild[] valChildArray) {
            this.valChildArray = valChildArray;
        }

        /**
         * @return Map.
         */
        @SuppressWarnings("unused")
        public Map getValMap() {
            return valMap;
        }

        /**
         * @param valMap Map.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValMap(Map valMap) {
            this.valMap = valMap;
        }

        /**
         * @return Child map.
         */
        @SuppressWarnings("unused")
        public Map<String, TestChild> getValChildMap() {
            return valChildMap;
        }

        /**
         * @param valChildMap Child map.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValChildMap(Map<String, TestChild> valChildMap) {
            this.valChildMap = valChildMap;
        }

        /**
         * @return Linked map.
         */
        @SuppressWarnings("unused")
        public LinkedHashMap getValLinkedHashMap() {
            return valLinkedHashMap;
        }

        /**
         * @param valLinkedHashMap Linked map.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValLinkedHashMap(LinkedHashMap valLinkedHashMap) {
            this.valLinkedHashMap = valLinkedHashMap;
        }

        /**
         * @return Collection.
         */
        @SuppressWarnings("unused")
        public Collection getValCollection() {
            return valCollection;
        }

        /**
         * @param valCollection Collection.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValCollection(Collection valCollection) {
            this.valCollection = valCollection;
        }

        /**
         * @return Child list.
         */
        @SuppressWarnings("unused")
        public List<TestChild> getValChildList() {
            return valChildList;
        }

        /**
         * @param valChildList Child list.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValChildList(List<TestChild> valChildList) {
            this.valChildList = valChildList;
        }

        /**
         * @return Linked list.
         */
        @SuppressWarnings("unused")
        public LinkedList getValLinkedList() {
            return valLinkedList;
        }

        /**
         * @param valLinkedList Linked list.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValLinkedList(LinkedList valLinkedList) {
            this.valLinkedList = valLinkedList;
        }

        /**
         * @return Child set.
         */
        @SuppressWarnings("unused")
        public Set<TestChild> getValChildSet() {
            return valChildSet;
        }

        /**
         * @param valChildSet Child set.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValChildSet(Set<TestChild> valChildSet) {
            this.valChildSet = valChildSet;
        }

        /**
         * @return Linked hash set.
         */
        @SuppressWarnings("unused")
        public LinkedHashSet getValLinkedHashSet() {
            return valLinkedHashSet;
        }

        /**
         * @param valLinkedHashSet Linked hash set.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValLinkedHashSet(LinkedHashSet valLinkedHashSet) {
            this.valLinkedHashSet = valLinkedHashSet;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return new StringBuilder("TestParent [\n")
                .append("valStringArray=[").append(Arrays.toString(valStringArray)).append("],\n")
                .append("valChildArray=[").append(Arrays.toString(valChildArray)).append("],\n")
                .append("valMap=[").append(valMap).append("],\n")
                .append("valChildMap=[").append(valChildMap).append("],\n")
                .append("valLinkedHashMap=[").append(valLinkedHashMap).append("],\n")
                .append("valCollection=[").append(valCollection).append("],\n")
                .append("valChildList=[").append(valChildList).append("],\n")
                .append("valLinkedList=[").append(valLinkedList).append("],\n")
                .append("valChildSet=[").append(valChildSet).append("],\n")
                .append("valLinkedHashSet=[").append(valLinkedHashSet).append("],\n")
                .append("]")
                .toString();
        }
    }

    /**
     *
     */
    @SuppressWarnings( {"AssignmentToDateFieldFromParameter", "ReturnOfDateField", "PublicInnerClass"})
    public static class TestChild {
        /** */
        private Double valDoubleObj;

        /** */
        private double valDouble;

        /** */
        private Float valFloatObj;

        /** */
        private float valFloat;

        /** */
        private Long valLongObj;

        /** */
        private long valLong;

        /** */
        private Integer valIntegerObj;

        /** */
        private int valInt;

        /** */
        private Boolean valBooleanObj;

        /** */
        private boolean valBoolean;

        /** */
        private String valString;

        /** */
        private Date valDate;

        /** */
        private Calendar valCalendar;

        /**
         * @return Double.
         */
        @SuppressWarnings("unused")
        public Double getValDoubleObj() {
            return valDoubleObj;
        }

        /**
         * @param valDoubleObj Double.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValDoubleObj(Double valDoubleObj) {
            this.valDoubleObj = valDoubleObj;
        }

        /**
         * @return Double.
         */
        @SuppressWarnings("unused")
        public double getValDouble() {
            return valDouble;
        }

        /**
         * @param valDouble Double.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValDouble(double valDouble) {
            this.valDouble = valDouble;
        }

        /**
         * @return Float.
         */
        @SuppressWarnings("unused")
        public Float getValFloatObj() {
            return valFloatObj;
        }

        /**
         * @param valFloatObj Float.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValFloatObj(Float valFloatObj) {
            this.valFloatObj = valFloatObj;
        }

        /**
         * @return Float.
         */
        @SuppressWarnings("unused")
        public float getValFloat() {
            return valFloat;
        }

        /**
         * @param valFloat Float.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValFloat(float valFloat) {
            this.valFloat = valFloat;
        }

        /**
         * @return Long.
         */
        @SuppressWarnings("unused")
        public Long getValLongObj() {
            return valLongObj;
        }

        /**
         * @param valLongObj Long.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValLongObj(Long valLongObj) {
            this.valLongObj = valLongObj;
        }

        /**
         * @return Long.
         */
        @SuppressWarnings("unused")
        public long getValLong() {
            return valLong;
        }

        /**
         * @param valLong Long.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValLong(long valLong) {
            this.valLong = valLong;
        }

        /**
         * @return Integer.
         */
        @SuppressWarnings("unused")
        public Integer getValIntegerObj() {
            return valIntegerObj;
        }

        /**
         * @param valIntegerObj Integer.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValIntegerObj(Integer valIntegerObj) {
            this.valIntegerObj = valIntegerObj;
        }

        /**
         * @return Integer.
         */
        @SuppressWarnings("unused")
        public int getValInt() {
            return valInt;
        }

        /**
         * @param valInt Integer.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValInt(int valInt) {
            this.valInt = valInt;
        }

        /**
         * @return Boolean.
         */
        @SuppressWarnings("unused")
        public Boolean getValBooleanObj() {
            return valBooleanObj;
        }

        /**
         * @param valBooleanObj Boolean.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValBooleanObj(Boolean valBooleanObj) {
            this.valBooleanObj = valBooleanObj;
        }

        /**
         * @return Boolean.
         */
        @SuppressWarnings("unused")
        public boolean isValBoolean() {
            return valBoolean;
        }

        /**
         * @param valBoolean Boolean.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValBoolean(boolean valBoolean) {
            this.valBoolean = valBoolean;
        }

        /**
         * @return String.
         */
        @SuppressWarnings("unused")
        public String getValString() {
            return valString;
        }

        /**
         * @param valString String.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValString(String valString) {
            this.valString = valString;
        }

        /**
         * @return Date.
         */
        @SuppressWarnings("unused")
        public Date getValDate() {
            return valDate;
        }

        /**
         * @param valDate Date.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValDate(Date valDate) {
            this.valDate = valDate;
        }

        /**
         * @return Calendar.
         */
        @SuppressWarnings("unused")
        public Calendar getValCalendar() {
            return valCalendar;
        }

        /**
         * @param valCalendar Calendar.
         */
        @SuppressWarnings("unused")
        @GridSpiConfiguration(optional = true)
        public void setValCalendar(Calendar valCalendar) {
            this.valCalendar = valCalendar;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return new StringBuilder("TestChild [")
                .append("valDoubleObj=").append(valDoubleObj).append(", ")
                .append("valDouble=").append(valDouble).append(", ")
                .append("valFloatObj=").append(valFloatObj).append(", ")
                .append("valFloat=").append(valFloat).append(", ")
                .append("valLongObj=").append(valLongObj).append(", ")
                .append("valLong=").append(valLong).append(", ")
                .append("valIntegerObj=").append(valIntegerObj).append(", ")
                .append("valInt=").append(valInt).append(", ")
                .append("valBooleanObj=").append(valBooleanObj).append(", ")
                .append("valBoolean=").append(valBoolean).append(", ")
                .append("valString=").append(valString).append(", ")
                .append("valDate=").append(valDate != null ?
                    DateFormatUtils.format(valDate, "yyyyMMddHHmmss") : null).append(", ")
                .append("valCalendar=").append(valCalendar != null ?
                    DateFormatUtils.format(valCalendar.getTime(), "yyyyMMddHHmmss") : null)
                .append("]")
                .toString();
        }
    }
}
