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

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/system/ui/custom_window.h"

using ignite::odbc::config::Configuration;

/**
 * Configuration window class.
 */
class ConfigurationWindow : public ignite::odbc::system::ui::CustomWindow
{
    enum ElementId
    {
        ID_NAME_EDIT,
        ID_OK_BUTTON,
        ID_CANCEL_BUTTON
    };
public:
    /**
     * Constructor.
     *
     * @param parent Parent window handle.
     */
    explicit ConfigurationWindow(Window* parent) :
        CustomWindow(parent, "IgniteConfigureDsn", "Configure Apache Ignite DSN"),
        nameEdit(),
        okButton(),
        cancelButton()
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    virtual ~ConfigurationWindow()
    {
        // No-op.
    }

    /**
     * Create window in the center of the parent window.
     */
    void Create()
    {
        // Finding out parent position.
        RECT parentRect;
        GetWindowRect(parent->GetHandle(), &parentRect);

        width = 320;
        height = 480;

        // Positioning window to the center of parent window.
        const int posX = parentRect.left + (parentRect.right - parentRect.left - width) / 2;
        const int posY = parentRect.top + (parentRect.bottom - parentRect.top - height) / 2;

        Window::Create(WS_OVERLAPPED | WS_SYSMENU, posX, posY, width, height, 0);

        if (!handle)
            throw ignite::IgniteError(GetLastError(), "Can not create window");
    }

    virtual void OnCreate()
    {
        nameEdit.reset(new Window(this, "Edit", ""));
        nameEdit->Create(WS_CHILD | WS_VISIBLE | WS_BORDER, 50, 50, 150, 20, ID_NAME_EDIT);

        okButton.reset(new Window(this, "Button", "Ok"));
        okButton->Create(WS_CHILD | WS_VISIBLE , 50, 100, 80, 25, ID_OK_BUTTON);

        cancelButton.reset(new Window(this, "Button", "Cancel"));
        cancelButton->Create(WS_CHILD | WS_VISIBLE, 190, 100, 80, 25, ID_CANCEL_BUTTON);
    }

    virtual bool OnMessage(UINT msg, WPARAM wParam, LPARAM lParam)
    {
        switch (msg)
        {
            case WM_COMMAND:
            {
                LOG_MSG("WM_COMMAND\n");

                switch (LOWORD(wParam))
                {
                    case ID_OK_BUTTON:
                    case ID_CANCEL_BUTTON:
                    {
                        DestroyWindow(GetHandle());

                        break;
                    }

                    default:
                        break;
                }

                break;
            }

            case WM_DESTROY:
            {
                LOG_MSG("WM_DESTROY\n");

                PostQuitMessage(0);

                break;
            }

            default:
                return false;
        }

        return true;
    }

private:
    std::auto_ptr<Window> nameEdit;

    std::auto_ptr<Window> okButton;
    std::auto_ptr<Window> cancelButton;
};

void DisplayAddDsnWindow(HWND hwndParent, Configuration& config)
{
    using namespace ignite::odbc::system::ui;

    if (!hwndParent)
        return;

    try
    {
        Window parent(hwndParent);

        ConfigurationWindow window(&parent);

        window.Create();

        window.Show();
        window.Update();

        ProcessMessages();
    }
    catch (const ignite::IgniteError& err)
    {
        std::stringstream buf;

        buf << "Message: " << err.GetText() << ", Code: " << err.GetCode();

        MessageBox(NULL, buf.str().c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);
    }
}

void DisplayConfigureDsnWindow(HWND hwndParent, Configuration& config)
{
    if (!hwndParent)
        return;

    //
}

BOOL INSTAPI ConfigDSN(HWND     hwndParent,
                       WORD     req,
                       LPCSTR   driver,
                       LPCSTR   attributes)
{
    LOG_MSG("ConfigDSN called\n");

    Configuration config;

    config.FillFromConfigAttributes(attributes);

    if (!SQLValidDSN(config.GetDsn().c_str()))
        return SQL_FALSE;

    LOG_MSG("Driver: %s\n", driver);
    LOG_MSG("Attributes: %s\n", attributes);

    LOG_MSG("DSN: %s\n", config.GetDsn().c_str());

    switch (req)
    {
        case ODBC_ADD_DSN:
        {
            LOG_MSG("ODBC_ADD_DSN\n");

            DisplayAddDsnWindow(hwndParent, config);

            return SQL_FALSE;// SQLWriteDSNToIni(config.GetDsn().c_str(), driver);
        }

        case ODBC_CONFIG_DSN:
        {
            LOG_MSG("ODBC_CONFIG_DSN\n");
            break;
        }

        case ODBC_REMOVE_DSN:
        {
            LOG_MSG("ODBC_REMOVE_DSN\n");

            return SQLRemoveDSNFromIni(config.GetDsn().c_str());
        }

        default:
        {
            return SQL_FALSE;
        }
    }

    return SQL_TRUE;
}

