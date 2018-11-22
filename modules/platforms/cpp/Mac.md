# Compiling on a Mac


0. I cheated. I wasn't interested in either the ODBC driver or the thin client. This meant
I was able to configure with:

```
glibtoolize && aclocal && autoheader && automake --add-missing && autoreconf
./configure --disable-odbc --disable-thin-client --disable-tests
```

Yeah, I also disabled tests. There were a lot of compile errors there too and I didn't want
to dedicate the time to fixing them all. (There's a good project here for anyone who's
interested!)

1. The Mac doesn't have `pthread_condattr_setclock`. As far as I can tell, this makes
Ignite use a more precise clock when waiting for locks. In the sense that people
are unlikely to use Macs as servers, it's probably safe to just remove these lines.

```
diff --git a/modules/platforms/cpp/common/os/linux/include/ignite/common/concurrent_os.h b/modules/platforms/cpp/common/os/linux/include/ignite/common/concurrent_os.h
index 84bc8e6146..60d339ab06 100644
--- a/modules/platforms/cpp/common/os/linux/include/ignite/common/concurrent_os.h
+++ b/modules/platforms/cpp/common/os/linux/include/ignite/common/concurrent_os.h
@@ -411,8 +411,8 @@ namespace ignite
                     int err = pthread_condattr_init(&attr);
                     assert(!err);
 
-                    err = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
-                    assert(!err);
+//                     err = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
+//                     assert(!err);
 
                     err = pthread_cond_init(&cond, &attr);
                     assert(!err);
@@ -502,8 +502,8 @@ namespace ignite
                     int err = pthread_condattr_init(&attr);
                     assert(!err);
 
-                    err = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
-                    assert(!err);
+//                     err = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
+//                     assert(!err);
 
                     err = pthread_cond_init(&cond, &attr);
                     assert(!err);
```

Disclaimer: don't blame me if your computer melts or otherwise malfunctions.

2. Depending on how you installed your JDK, you might find that the build system
can't find some JNI components. I "fixed" this by manually adding an include
path on the command line:

```
CPLUS_INCLUDE_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include/darwin  make -j 8
```

3. Currently, Ignite is hard-coded to use `amd64` in a few places. On the Mac there
seems to be either no architecture or it uses `darwin` instead.

```
diff --git a/modules/platforms/cpp/jni/Makefile.am b/modules/platforms/cpp/jni/Makefile.am
index 56eaa6c844..eaebe99c1d 100644
--- a/modules/platforms/cpp/jni/Makefile.am
+++ b/modules/platforms/cpp/jni/Makefile.am
@@ -38,7 +38,7 @@ AM_CXXFLAGS = \
     -std=c++03
 
 libignite_jni_la_LIBADD = \
-    -L$(JAVA_HOME)/jre/lib/amd64/server \
+    -L$(JAVA_HOME)/jre/lib/server \
     @top_srcdir@/common/libignite-common.la
 
 libignite_jni_la_LDFLAGS = \
diff --git a/modules/platforms/cpp/jni/os/linux/src/utils.cpp b/modules/platforms/cpp/jni/os/linux/src/utils.cpp
index 52e4097b67..cca37fdf34 100644
--- a/modules/platforms/cpp/jni/os/linux/src/utils.cpp
+++ b/modules/platforms/cpp/jni/os/linux/src/utils.cpp
@@ -37,7 +37,7 @@ namespace ignite
     namespace jni
     {
         const char* JAVA_HOME = "JAVA_HOME";
-        const char* JAVA_DLL = "/jre/lib/amd64/server/libjvm.so";
+        const char* JAVA_DLL = "/jre/lib/server/libjvm.dylib";
 
         const char* IGNITE_HOME = "IGNITE_HOME";
 
```

4. And then to run it you need to specify a library path:

```
LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/server ignite/ignite 
```

Final note: as per the documentation, if you want to run a mixture of nodes (i.e., with 
both C++ and Java), you'll need to make the following configuration change on the Java side:

```
        <property name="binaryConfiguration">
            <bean class="org.apache.ignite.configuration.BinaryConfiguration">
                <property name="compactFooter" value="false"/>

                <property name="idMapper">
                    <bean class="org.apache.ignite.binary.BinaryBasicIdMapper">
                        <property name="lowerCase" value="true"/>
                    </bean>
                </property>
            </bean>
        </property>
```
