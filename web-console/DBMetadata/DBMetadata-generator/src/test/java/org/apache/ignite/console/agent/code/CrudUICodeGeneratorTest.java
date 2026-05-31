package org.apache.ignite.console.agent.code;


import io.vertx.core.json.JsonObject;

public class CrudUICodeGeneratorTest {

    CrudUICodeGenerator gen = new CrudUICodeGenerator();

    public void setUp() throws Exception {
    }

    public void tearDown() throws Exception {
    }

    public void testGenCode(){
        JsonObject context = new JsonObject("");
        gen.generator("/tmp/code",context.getMap(),null);
    }

    public static void main(String[] args) throws Exception {
        CrudUICodeGeneratorTest case1 = new CrudUICodeGeneratorTest();
        case1.setUp();
        case1.testGenCode();
        case1.tearDown();

    }
}