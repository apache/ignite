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
                void ProcessMessages()
                {
                    MSG msg;

                    while (GetMessage(&msg, NULL, 0, 0) > 0)
                    {
                        TranslateMessage(&msg);

                        DispatchMessage(&msg);
                    }
                }

                LRESULT CALLBACK Window::WndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
                {
                    Window* window = reinterpret_cast<Window*>(GetWindowLongPtr(hwnd, GWLP_USERDATA));

                    switch (msg)
                    {
                        case WM_NCCREATE:
                        {
                            _ASSERT(lParam != NULL);

                            CREATESTRUCT* createStruct = reinterpret_cast<CREATESTRUCT*>(lParam);

                            LONG_PTR longSelfPtr = reinterpret_cast<LONG_PTR>(createStruct->lpCreateParams);

                            SetWindowLongPtr(hwnd, GWLP_USERDATA, longSelfPtr);

                            return DefWindowProc(hwnd, msg, wParam, lParam);
                        }

                        case WM_CREATE:
                        {
                            _ASSERT(window != NULL);

                            window->SetHandle(hwnd);

                            window->OnCreate();

                            return 0;
                        }

                        default:
                            break;
                    }

                    if (window && window->OnMessage(msg, wParam, lParam))
                        return 0;

                    return DefWindowProc(hwnd, msg, wParam, lParam);
                }

                Window::Window(HWND parent, const char* className, const char* title) :
                    className(className),
                    title(title),
                    handle(NULL),
                    parentHandle(parent),
                    width(0),
                    height(0)
                {
                    WNDCLASS wcx;

                    wcx.style = CS_HREDRAW | CS_VREDRAW;
                    wcx.lpfnWndProc = WndProc;
                    wcx.cbClsExtra = 0;
                    wcx.cbWndExtra = 0;
                    wcx.hInstance = GetHInstance();
                    wcx.hIcon = NULL;
                    wcx.hCursor = NULL;
                    wcx.hbrBackground = NULL;
                    wcx.lpszMenuName = NULL;
                    wcx.lpszClassName = className;

                    if (!RegisterClass(&wcx))
                        throw IgniteError(GetLastError(), "Can not register window class");
                }

                Window::~Window()
                {
                    if (handle)
                        DestroyWindow(handle);

                    UnregisterClass(className.c_str(), GetHInstance());
                }

                void Window::Create(int width, int height)
                {
                    this->width = width;
                    this->height = height;

                    // Finding out parent position.
                    RECT parentRect;
                    GetWindowRect(parentHandle, &parentRect);

                    // Positioning window to the center of parent window.
                    const int posX = parentRect.left + (parentRect.right - parentRect.left - width) / 2;
                    const int posY = parentRect.top + (parentRect.bottom - parentRect.top - height) / 2;
                    
                    handle = CreateWindow(
                        className.c_str(),
                        title.c_str(),
                        WS_OVERLAPPED | WS_SYSMENU,
                        posX,
                        posY,
                        width,
                        height,
                        parentHandle,
                        NULL,
                        GetHInstance(),
                        this
                    );

                    if (!handle)
                        throw IgniteError(GetLastError(), "Can not create window");
                }

                void Window::Show()
                {
                    ShowWindow(handle, SW_SHOW);
                }

                void Window::Update()
                {
                    UpdateWindow(handle);
                }

                HINSTANCE Window::GetHInstance()
                {
                    HINSTANCE hInstance = GetModuleHandle(TARGET_MODULE_FULL_NAME);

                    if (hInstance == NULL)
                        throw IgniteError(GetLastError(), "Can not get hInstance for the module.");

                    return hInstance;
                }
            }
        }
    }
}
