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


LRESULT CALLBACK WndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    switch (msg)
    {
        case WM_CLOSE:
        {
            DestroyWindow(hwnd);
            break;
        }

        case WM_DESTROY:
        {
            PostQuitMessage(0);
            break;
        }

        default:
            return DefWindowProc(hwnd, msg, wParam, lParam);
    }
    return 0;
}

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                Window::Window(HWND parent, const char* className, const char* title) :
                    className(className),
                    title(title),
                    parentHandle(parent)
                {
                    WNDCLASS wcx;

                    HINSTANCE hInstance = GetHInstance();

                    if (hInstance == NULL)
                        throw IgniteError(GetLastError(), "Can not get hInstance for the module.");

                    wcx.style = CS_HREDRAW | CS_VREDRAW;
                    wcx.lpfnWndProc = WndProc;
                    wcx.cbClsExtra = 0;
                    wcx.cbWndExtra = 0;
                    wcx.hInstance = hInstance;
                    wcx.hIcon = NULL;
                    wcx.hCursor = NULL;
                    wcx.hbrBackground = NULL;
                    wcx.lpszMenuName = NULL;
                    wcx.lpszClassName = className;

                    if (!RegisterClass(&wcx))
                        throw IgniteError(GetLastError(), "Can not register window class");

                    // Finding out parent position.
                    RECT parentRect;
                    GetWindowRect(parent, &parentRect);

                    const int sizeX = 320;
                    const int sizeY = 480;

                    // Positioning window to the center of parent window.
                    const int posX = parentRect.left + (parentRect.right - parentRect.left - sizeX) / 2;
                    const int posY = parentRect.top + (parentRect.bottom - parentRect.top - sizeY) / 2;

                    handle = CreateWindow(
                        className,
                        title,
                        WS_OVERLAPPED | WS_SYSMENU,
                        posX,
                        posY,
                        sizeX,
                        sizeY,
                        parentHandle,
                        NULL,
                        hInstance,
                        NULL
                    );

                    if (!handle)
                    {
                        UnregisterClass(className, hInstance);

                        throw IgniteError(GetLastError(), "Can not create window");
                    }
                }

                Window::~Window()
                {
                    if (handle)
                        DestroyWindow(handle);

                    UnregisterClass(className.c_str(), GetHInstance());
                }

                void Window::Show()
                {
                    ShowWindow(handle, SW_SHOW);
                }

                void Window::Update()
                {
                    UpdateWindow(handle);
                }

                void Window::ProcessMessages()
                {
                    MSG msg;

                    while (GetMessage(&msg, NULL, 0, 0) > 0)
                    {
                        TranslateMessage(&msg);

                        DispatchMessage(&msg);
                    }
                }

                HINSTANCE Window::GetHInstance()
                {
                    return GetModuleHandle("ignite.odbc.dll");
                }
            }
        }
    }
}
