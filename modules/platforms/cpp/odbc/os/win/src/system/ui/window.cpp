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

#include "ignite/odbc/utility.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/system/ui/window.h"

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                HINSTANCE GetHInstance()
                {
                    HINSTANCE hInstance = GetModuleHandle(TARGET_MODULE_FULL_NAME);

                    if (hInstance == NULL)
                        throw IgniteError(GetLastError(), "Can not get hInstance for the module.");

                    return hInstance;
                }

                Window::Window(Window* parent, const char* className, const char* title) :
                    className(className),
                    title(title),
                    handle(NULL),
                    created(false),
                    parent(parent)
                {
                    // No-op.
                }

                Window::Window(HWND handle) :
                    className(),
                    title(),
                    handle(handle),
                    created(false),
                    parent(0)
                {
                    // No-op.
                }

                Window::~Window()
                {
                    if (created)
                        Destroy();
                }

                void Window::Create(DWORD style, int posX, int posY, int width, int height, int id)
                {
                    if (handle)
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Window already created");

                    handle = CreateWindow(
                        className.c_str(),
                        title.c_str(),
                        style,
                        posX,
                        posY,
                        width,
                        height,
                        parent ? parent->GetHandle() : NULL,
                        reinterpret_cast<HMENU>(id),
                        GetHInstance(),
                        this
                    );

                    if (!handle)
                        throw IgniteError(GetLastError(), "Can not create window");

                    created = true;

                    HGDIOBJ hfDefault = GetStockObject(DEFAULT_GUI_FONT);

                    SendMessage(GetHandle(), WM_SETFONT, (WPARAM)hfDefault, MAKELPARAM(FALSE, 0));
                }

                void Window::Show()
                {
                    ShowWindow(handle, SW_SHOW);
                }

                void Window::Update()
                {
                    UpdateWindow(handle);
                }

                void Window::Destroy()
                {
                    if (handle)
                        DestroyWindow(handle);

                    handle = NULL;
                }
            }
        }
    }
}
