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

#include <Windowsx.h>

#include "ignite/odbc/system/ui/custom_window.h"

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                Result::Type ProcessMessages(Window& window)
                {
                    MSG msg;

                    while (GetMessage(&msg, NULL, 0, 0) > 0)
                    {
                        if (!IsDialogMessage(window.GetHandle(), &msg))
                        {
                            TranslateMessage(&msg);

                            DispatchMessage(&msg);
                        }
                    }

                    return static_cast<Result::Type>(msg.wParam);
                }

                LRESULT CALLBACK CustomWindow::WndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
                {
                    CustomWindow* window = reinterpret_cast<CustomWindow*>(GetWindowLongPtr(hwnd, GWLP_USERDATA));

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

                CustomWindow::CustomWindow(Window* parent, const char* className, const char* title) :
                    Window(parent, className, title)
                {
                    WNDCLASS wcx;

                    wcx.style = CS_HREDRAW | CS_VREDRAW;
                    wcx.lpfnWndProc = WndProc;
                    wcx.cbClsExtra = 0;
                    wcx.cbWndExtra = 0;
                    wcx.hInstance = GetHInstance();
                    wcx.hIcon = NULL;
                    wcx.hCursor = LoadCursor(NULL, IDC_ARROW);
                    wcx.hbrBackground = (HBRUSH)COLOR_WINDOW;
                    wcx.lpszMenuName = NULL;
                    wcx.lpszClassName = className;


                    if (!RegisterClass(&wcx))
                    {
                        std::stringstream buf;

                        buf << "Can not register window class, error code: " << GetLastError();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
                    }
                }

                CustomWindow::~CustomWindow()
                {
                    UnregisterClass(className.c_str(), GetHInstance());
                }

                std::auto_ptr<Window> CustomWindow::CreateGroupBox(int posX, int posY,
                    int sizeX, int sizeY, const char* title, int id)
                {
                    std::auto_ptr<Window> child(new Window(this, "Button", title));

                    child->Create(WS_CHILD | WS_VISIBLE | BS_GROUPBOX, posX, posY, sizeX, sizeY, id);

                    return child;
                }

                std::auto_ptr<Window> CustomWindow::CreateLabel(int posX, int posY,
                    int sizeX, int sizeY, const char* title, int id)
                {
                    std::auto_ptr<Window> child(new Window(this, "Static", title));

                    child->Create(WS_CHILD | WS_VISIBLE, posX, posY, sizeX, sizeY, id);

                    return child;
                }

                std::auto_ptr<Window> CustomWindow::CreateEdit(int posX, int posY,
                    int sizeX, int sizeY, const char* title, int id, int style)
                {
                    std::auto_ptr<Window> child(new Window(this, "Edit", title));

                    child->Create(WS_CHILD | WS_VISIBLE | WS_BORDER | ES_AUTOHSCROLL | WS_TABSTOP | style,
                        posX, posY, sizeX, sizeY, id);

                    return child;
                }

                std::auto_ptr<Window> CustomWindow::CreateButton(int posX, int posY,
                    int sizeX, int sizeY, const char* title, int id)
                {
                    std::auto_ptr<Window> child(new Window(this, "Button", title));

                    child->Create(WS_CHILD | WS_VISIBLE | WS_TABSTOP, posX, posY, sizeX, sizeY, id);

                    return child;
                }

                std::auto_ptr<Window> CustomWindow::CreateCheckBox(int posX, int posY,
                    int sizeX, int sizeY, const char* title, int id, bool state)
                {
                    std::auto_ptr<Window> child(new Window(this, "Button", title));

                    child->Create(WS_CHILD | WS_VISIBLE | BS_CHECKBOX, posX, posY, sizeX, sizeY, id);

                    child->SetChecked(state);

                    return child;
                }

                std::auto_ptr<Window> CustomWindow::CreateComboBox(int posX, int posY,
                    int sizeX, int sizeY, const char * title, int id)
                {
                    std::auto_ptr<Window> child(new Window(this, "Combobox", title));

                    child->Create(WS_CHILD | WS_VISIBLE | CBS_DROPDOWNLIST, posX, posY, sizeX, sizeY, id);

                    return child;
                }
            }
        }
    }
}
