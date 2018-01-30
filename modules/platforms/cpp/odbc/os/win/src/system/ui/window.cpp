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
                    {
                        std::stringstream buf;

                        buf << "Can not get hInstance for the module, error code: " << GetLastError();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
                    }

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
                    {
                        std::stringstream buf;

                        buf << "Window already created, error code: " << GetLastError();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
                    }

                    handle = CreateWindow(
                        className.c_str(),
                        title.c_str(),
                        style,
                        posX,
                        posY,
                        width,
                        height,
                        parent ? parent->GetHandle() : NULL,
                        reinterpret_cast<HMENU>(static_cast<ptrdiff_t>(id)),
                        GetHInstance(),
                        this
                    );

                    if (!handle)
                    {
                        std::stringstream buf;

                        buf << "Can not create window, error code: " << GetLastError();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
                    }

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

                void Window::GetText(std::string& text) const
                {
                    if (!IsEnabled())
                    {
                        text.clear();

                        return;
                    }

                    int len = GetWindowTextLength(handle);

                    if (len <= 0)
                    {
                        text.clear();

                        return;
                    }

                    text.resize(len + 1);

                    if (!GetWindowText(handle, &text[0], len + 1))
                        text.clear();

                    text.resize(len);
                }

                void Window::SetText(const std::string& text) const
                {
                    SNDMSG(handle, WM_SETTEXT, 0, reinterpret_cast<LPARAM>(text.c_str()));
                }

                bool Window::IsChecked() const
                {
                    return IsEnabled() && Button_GetCheck(handle) == BST_CHECKED;
                }

                void Window::SetChecked(bool state)
                {
                    Button_SetCheck(handle, state ? BST_CHECKED : BST_UNCHECKED);
                }

                void Window::AddString(const std::string & str)
                {
                    SNDMSG(handle, CB_ADDSTRING, 0, reinterpret_cast<LPARAM>(str.c_str()));
                }

                void Window::SetSelection(int idx)
                {
                    SNDMSG(handle, CB_SETCURSEL, static_cast<WPARAM>(idx), 0);
                }

                int Window::GetSelection() const
                {
                    return static_cast<int>(SNDMSG(handle, CB_GETCURSEL, 0, 0));
                }

                void Window::SetEnabled(bool enabled)
                {
                    EnableWindow(GetHandle(), enabled);
                }

                bool Window::IsEnabled() const
                {
                    return IsWindowEnabled(GetHandle()) != 0;
                }
            }
        }
    }
}
