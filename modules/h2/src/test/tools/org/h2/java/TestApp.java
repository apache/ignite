/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

/**
 * A test application.
 */
public class TestApp {

/* c:

int main(int argc, char** argv) {
//    org_h2_java_TestApp_main(0);
    org_h2_java_TestApp_main(ptr<array<ptr<java_lang_String> > >());
}

*/

    /**
     * Run this application.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) {
        String[] list = new String[1000];
        for (int i = 0; i < 1000; i++) {
            list[i] = "Hello " + i;
        }

        // time:29244000 mac g++ -O3 without array bound checks
        // time:30673000 mac java
        // time:32449000 mac g++ -O3
        // time:69692000 mac g++ -O3 ref counted
        // time:1200000000 raspberry g++ -O3
        // time:1720000000 raspberry g++ -O3 ref counted
        // time:1980469000 raspberry java IcedTea6 1.8.13 Cacao VM
        // time:12962645810 raspberry java IcedTea6 1.8.13 Zero VM
        // java -XXaltjvm=cacao

        for (int k = 0; k < 4; k++) {
            long t = System.nanoTime();
            long h = 0;
            for (int j = 0; j < 10000; j++) {
                for (int i = 0; i < 1000; i++) {
                    String s = list[i];
                    h = (h * 7) ^ s.hashCode();
                }
            }
            System.out.println("hash: " + h);
            t = System.nanoTime() - t;
            System.out.println("time:" + t);
        }
    }

}
