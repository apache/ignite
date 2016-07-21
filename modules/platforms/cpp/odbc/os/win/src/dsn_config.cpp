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
#include "ignite/odbc/system/ui/dsn_configuration_window.h"

using ignite::odbc::config::Configuration;

#define BUFFER_SIZE 1024
#define CONFIG_FILE "ODBC.INI"

namespace odbc
{

    void ThrowLastError()
    {
        DWORD code;
        char msg[BUFFER_SIZE];

        SQLInstallerError(1, &code, msg, sizeof(msg), NULL);

        std::stringstream buf;

        buf << "Message: \"" << msg << "\", Code: " << code;

        throw ignite::IgniteError(ignite::IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
    }

    void AddStringToDsn(const char* dsn, const char* key, const char* value)
    {
        if (!SQLWritePrivateProfileString(dsn, key, value, CONFIG_FILE))
            ThrowLastError();
    }

    std::string GetDsnString(const char* dsn, const char* key, const char* dflt)
    {
        char buf[BUFFER_SIZE];

        memset(buf, 0, sizeof(buf));

        SQLGetPrivateProfileString(dsn, key, dflt, buf, sizeof(buf), CONFIG_FILE);

        return std::string(buf);
    }

    bool DisplayConfigureDsnWindow(HWND hwndParent, Configuration& config)
    {
        using namespace ignite::odbc::system::ui;

        if (!hwndParent)
            return false;

        try
        {
            Window parent(hwndParent);

            DsnConfigurationWindow window(&parent, config);

            window.Create();

            window.Show();
            window.Update();

            return ProcessMessages() == RESULT_OK;
        }
        catch (const ignite::IgniteError& err)
        {
            std::stringstream buf;

            buf << "Message: " << err.GetText() << ", Code: " << err.GetCode();

            std::string message = buf.str();

            MessageBox(NULL, message.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);

            SQLPostInstallerError(err.GetCode(), err.GetText());
        }

        return false;
    }

    bool RegisterDsn(const Configuration& config, LPCSTR driver)
    {
        using namespace ignite::odbc::config;
        using ignite::common::LexicalCast;

        const char* dsn = config.GetDsn().c_str();

        try
        {
            if (!SQLWriteDSNToIni(dsn, driver))
                ThrowLastError();

            AddStringToDsn(dsn, attrkey::host.c_str(), config.GetHost().c_str());
            AddStringToDsn(dsn, attrkey::port.c_str(), LexicalCast<std::string>(config.GetTcpPort()).c_str());
            AddStringToDsn(dsn, attrkey::cache.c_str(), config.GetCache().c_str());

            return true;
        }
        catch (ignite::IgniteError& err)
        {
            MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);

            SQLPostInstallerError(err.GetCode(), err.GetText());
        }

        return false;
    }

    bool UnregisterDsn(const char* dsn)
    {
        try
        {
            if (!SQLRemoveDSNFromIni(dsn))
                ThrowLastError();

            return true;
        }
        catch (ignite::IgniteError& err)
        {
            MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);

            SQLPostInstallerError(err.GetCode(), err.GetText());
        }

        return false;
    }

    void ReadDsnConfiguration(const char* dsn, Configuration& config)
    {
        using namespace ignite::odbc::config;
        using ignite::common::LexicalCast;

        std::string host = GetDsnString(dsn, attrkey::host.c_str(), config.GetHost().c_str());
        std::string port = GetDsnString(dsn, attrkey::port.c_str(), LexicalCast<std::string>(config.GetTcpPort()).c_str());
        std::string cache = GetDsnString(dsn, attrkey::cache.c_str(), config.GetCache().c_str());

        config.SetHost(host);
        config.SetTcpPort(LexicalCast<uint16_t>(port));
        config.SetCache(cache);
    }
}

BOOL INSTAPI ConfigDSN(HWND     hwndParent,
                       WORD     req,
                       LPCSTR   driver,
                       LPCSTR   attributes)
{
    LOG_MSG("ConfigDSN called\n");

    Configuration config;

    LOG_MSG("Attributes: %s\n", attributes);

    config.FillFromConfigAttributes(attributes);

    if (!SQLValidDSN(config.GetDsn().c_str()))
        return FALSE;

    LOG_MSG("Driver: %s\n", driver);
    LOG_MSG("Attributes: %s\n", attributes);

    LOG_MSG("DSN: %s\n", config.GetDsn().c_str());

    switch (req)
    {
        case ODBC_ADD_DSN:
        {
            LOG_MSG("ODBC_ADD_DSN\n");

            if (!odbc::DisplayConfigureDsnWindow(hwndParent, config))
                return FALSE;

            if (!odbc::RegisterDsn(config, driver))
                return FALSE;

            break;
        }

        case ODBC_CONFIG_DSN:
        {
            LOG_MSG("ODBC_CONFIG_DSN\n");

            std::string dsn = config.GetDsn();

            Configuration loaded(config);

            odbc::ReadDsnConfiguration(dsn.c_str(), loaded);

            if (!odbc::DisplayConfigureDsnWindow(hwndParent, loaded))
                return FALSE;

            if (!odbc::RegisterDsn(loaded, driver))
                return FALSE;

            if (loaded.GetDsn() != dsn && !odbc::UnregisterDsn(dsn.c_str()))
                return FALSE;

            break;
        }

        case ODBC_REMOVE_DSN:
        {
            LOG_MSG("ODBC_REMOVE_DSN\n");

            if (!odbc::UnregisterDsn(config.GetDsn().c_str()))
                return FALSE;

            break;
        }

        default:
            return FALSE;
    }

    return TRUE;
}