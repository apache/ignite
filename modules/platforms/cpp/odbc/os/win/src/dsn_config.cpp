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
#include "ignite/odbc/system/ui/window.h"

using ignite::odbc::config::Configuration;


void DisplayAddDsnWindow(HWND hwndParent, Configuration& config)
{
    using ignite::odbc::system::ui::Window;

    if (!hwndParent)
        return;

    try
    {
        Window window(hwndParent, "IgniteConfigureDsn", "Configure Apache Ignite DSN");

        window.Show();
        window.Update();

        Window::ProcessMessages();
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

            return SQLWriteDSNToIni(config.GetDsn().c_str(), driver);
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

