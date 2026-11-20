# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from launcher_contracts import launcher_changes, supports_launcher


UNIX = '''checkJava() {
    javaMajorVersion "$JAVA"

    if [ $version -lt 17 ]; then
        echo "Please point JAVA_HOME variable to installation of JDK 17 or later."
        exit 1
    fi
}
'''
WINDOWS = '''if exist "%JAVA_HOME%\\bin\\java.exe" goto checkJdkVersion
:checkJdkVersion
if %MAJOR_JAVA_VER% == 1 set MAJOR_JAVA_VER=%MINOR_JAVA_VER%

if %MAJOR_JAVA_VER% LSS 17 (
    echo Please point JAVA_HOME variable to installation of JDK 17 or later.
    goto error_finish
)
:error_finish
goto :eof
'''


class LauncherContractsTest(unittest.TestCase):
    def test_launcher_rejects_a_different_java_version(self):
        for path, script in (("bin/include/functions.sh", UNIX), ("bin/ignite.bat", WINDOWS),
                             ("bin/index-reader.bat", WINDOWS), ("bin/control.bat", WINDOWS.replace("\n", "\r\n"))):
            with self.subTest(path=path):
                self.assertTrue(supports_launcher(path))
                self.assertEqual(launcher_changes(path, script, script.replace("17", "21")),
                                 ["Changed minimum Java version accepted by the launcher from 17 to 21."])

    def test_message_or_another_version_check_is_not_a_startup_requirement(self):
        self.assertFalse(supports_launcher("tools/test.bat"))
        self.assertEqual(launcher_changes("tools/test.bat", WINDOWS, WINDOWS.replace("17", "21")), [])
        self.assertEqual(launcher_changes("bin/include/functions.sh", UNIX, UNIX.replace("JDK 17", "JDK 21")), [])
        for old, new in (("exit 1", "exit 0"), ("exit 1", "return"), ("-lt", "-eq"),
                         ("checkJava()", "printExample()")):
            with self.subTest(edit=new):
                edited = UNIX.replace(old, new).replace("17", "21")
                self.assertEqual(launcher_changes("bin/include/functions.sh", UNIX, edited), [])
        self.assertEqual(launcher_changes("bin/control.bat", WINDOWS,
                                          WINDOWS.replace("17", "21").replace("goto error_finish", "echo error")), [])
        continues = WINDOWS.replace("goto :eof", "goto launch")
        self.assertEqual(launcher_changes("bin/control.bat", continues, continues.replace("17", "21")), [])
