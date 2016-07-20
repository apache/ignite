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
        ID_CONNECTION_SETTINGS_GROUP_BOX,
        ID_NAME_EDIT,
        ID_NAME_LABEL,
        ID_SERVER_EDIT,
        ID_SERVER_LABEL,
        ID_PORT_EDIT,
        ID_PORT_LABEL,
        ID_CACHE_EDIT,
        ID_CACHE_LABEL,
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
        width(320),
        height(200),
        nameStatic(),
        nameEdit(),
        serverStatic(),
        serverEdit(),
        portStatic(),
        portEdit(),
        cacheStatic(),
        cacheEdit(),
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

        // Positioning window to the center of parent window.
        const int posX = parentRect.left + (parentRect.right - parentRect.left - width) / 2;
        const int posY = parentRect.top + (parentRect.bottom - parentRect.top - height) / 2;

        RECT desiredRect = { posX, posY, posX + width, posY + height };
        AdjustWindowRect(&desiredRect, WS_BORDER | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME, FALSE);

        Window::Create(WS_OVERLAPPED | WS_SYSMENU, desiredRect.left, desiredRect.top,
            desiredRect.right - desiredRect.left, desiredRect.bottom - desiredRect.top, 0);

        if (!handle)
            throw ignite::IgniteError(GetLastError(), "Can not create window");
    }

    virtual void OnCreate()
    {
        int margin = 10;
        int interval = 10;

        int labelSizeX = 60;
        int labelPosX = margin + interval;

        int editSizeX = width - labelSizeX - 2 * margin - 3 * interval;
        int editPosX = margin + labelSizeX + 2 * interval;

        int rowSize = 20;
        int rowPos = margin + 2 * interval;

        nameStatic = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "DSN name:", ID_NAME_LABEL);
        nameEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, "", ID_NAME_EDIT);

        rowPos += interval + rowSize;

        serverStatic = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Server:", ID_SERVER_LABEL);
        serverEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, "", ID_SERVER_EDIT);

        rowPos += interval + rowSize;

        portStatic = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Port:", ID_PORT_LABEL);
        portEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, "10800", ID_PORT_EDIT);

        rowPos += interval + rowSize;

        cacheStatic = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Cache:", ID_CACHE_LABEL);
        cacheEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, "", ID_CACHE_EDIT);

        rowPos += interval * 2 + rowSize;
        rowSize = 25;

        connectionSettingsGroupBox = CreateGroupBox(margin, margin,
            width - 2 * margin, rowPos - 2 * margin, "Connection settings", ID_CONNECTION_SETTINGS_GROUP_BOX);

        int buttonSizeX = 80;
        int buttonCancelPosX = width - margin - buttonSizeX;
        int buttonOkPosX = buttonCancelPosX - interval - buttonSizeX;

        okButton = CreateButton(buttonOkPosX, rowPos, buttonSizeX, rowSize, "Ok", ID_OK_BUTTON);
        cancelButton = CreateButton(buttonCancelPosX, rowPos, buttonSizeX, rowSize, "Cancel", ID_CANCEL_BUTTON);
    }

    std::auto_ptr<Window> CreateGroupBox(int posX, int posY, int sizeX, int sizeY, const char* title, int id)
    {
        std::auto_ptr<Window> child(new Window(this, "Button", title));

        child->Create(WS_CHILD | WS_VISIBLE | BS_GROUPBOX, posX, posY, sizeX, sizeY, id);

        return child;
    }

    std::auto_ptr<Window> CreateLabel(int posX, int posY, int sizeX, int sizeY, const char* title, int id)
    {
        std::auto_ptr<Window> child(new Window(this, "Static", title));

        child->Create(WS_CHILD | WS_VISIBLE, posX, posY, sizeX, sizeY, id);

        return child;
    }

    std::auto_ptr<Window> CreateEdit(int posX, int posY, int sizeX, int sizeY, const char* title, int id)
    {
        std::auto_ptr<Window> child(new Window(this, "Edit", title));

        child->Create(WS_CHILD | WS_VISIBLE | WS_BORDER, posX, posY, sizeX, sizeY, id);

        return child;
    }

    std::auto_ptr<Window> CreateButton(int posX, int posY, int sizeX, int sizeY, const char* title, int id)
    {
        std::auto_ptr<Window> child(new Window(this, "Button", title));

        child->Create(WS_CHILD | WS_VISIBLE, posX, posY, sizeX, sizeY, id);

        return child;
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
    /** Window width. */
    int width;

    /** Window height. */
    int height;

    std::auto_ptr<Window> connectionSettingsGroupBox;

    std::auto_ptr<Window> nameStatic;
    std::auto_ptr<Window> nameEdit;

    std::auto_ptr<Window> serverStatic;
    std::auto_ptr<Window> serverEdit;

    std::auto_ptr<Window> portStatic;
    std::auto_ptr<Window> portEdit;

    std::auto_ptr<Window> cacheStatic;
    std::auto_ptr<Window> cacheEdit;

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

