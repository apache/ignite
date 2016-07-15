/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _IGNITE_ODBC_SYSTEM_UI_WINDOW
#define _IGNITE_ODBC_SYSTEM_UI_WINDOW

#include "ignite/odbc/utility.h"
#include "ignite/odbc/system/odbc_constants.h"

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                class Window
                {
                public:
                    Window(HWND parent, const char* className, const char* title);

                    ~Window();

                    void Show();

                    void Update();

                    static void ProcessMessages();

                private:
                    static HINSTANCE GetHInstance();

                    /** Window class name. */
                    std::string className;

                    /** Window title. */
                    std::string title;

                    /** Window handle */
                    HWND handle;

                    /** Window parent handle */
                    HWND parentHandle;
                };
            }
        }
    }
}

#endif //_IGNITE_ODBC_SYSTEM_UI_WINDOW