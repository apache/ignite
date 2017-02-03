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
#include "ignite/odbc/log.h"
#include "ignite/odbc/system/odbc_constants.h"

#include "ignite/odbc/dsn_config.h"
#include "ignite/odbc/system/ui/window.h"
#include "ignite/odbc/system/ui/dsn_configuration_window.h"

using ignite::odbc::config::Configuration;

/**
 * Display configuration window for user to configure DSN.
 *
 * @param hwndParent Parent window handle.
 * @param config Output configuration.
 * @return True on success and false on fail.
 */
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

        return ProcessMessages(window) == RESULT_OK;
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

/**
 * Register DSN with specified configuration.
 *
 * @param config Configuration.
 * @param driver Driver.
 * @return True on success and false on fail.
 */
bool RegisterDsn(const Configuration& config, LPCSTR driver)
{
    using namespace ignite::odbc::config;
    using ignite::common::LexicalCast;

    typedef Configuration::ArgumentMap ArgMap;

    const char* dsn = config.GetDsn().c_str();

    try
    {
        if (!SQLWriteDSNToIni(dsn, driver))
            ignite::odbc::ThrowLastSetupError();

        const ArgMap& map = config.GetMap();

        std::set<std::string> ignore;

        ignore.insert(Configuration::Key::dsn);
        ignore.insert(Configuration::Key::driver);

        for (ArgMap::const_iterator it = map.begin(); it != map.end(); ++it)
        {
            const std::string& key = it->first;
            const std::string& value = it->second;

            if (ignore.find(key) != ignore.end())
                continue;

            ignite::odbc::WriteDsnString(dsn, key.c_str(), value.c_str());
        }

        return true;
    }
    catch (ignite::IgniteError& err)
    {
        MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);

        SQLPostInstallerError(err.GetCode(), err.GetText());
    }

    return false;
}

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const char* dsn)
{
    try
    {
        if (!SQLRemoveDSNFromIni(dsn))
            ignite::odbc::ThrowLastSetupError();

        return true;
    }
    catch (ignite::IgniteError& err)
    {
        MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);

        SQLPostInstallerError(err.GetCode(), err.GetText());
    }

    return false;
}

BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD req, LPCSTR driver, LPCSTR attributes)
{
    using namespace ignite::odbc;

    LOG_MSG("ConfigDSN called");

    Configuration config;

    LOG_MSG("Attributes: " << attributes);

    config.FillFromConfigAttributes(attributes);

    if (!SQLValidDSN(config.GetDsn().c_str()))
        return FALSE;

    LOG_MSG("Driver: " << driver);
    LOG_MSG("Attributes: " << attributes);

    LOG_MSG("DSN: " << config.GetDsn());

    switch (req)
    {
        case ODBC_ADD_DSN:
        {
            LOG_MSG("ODBC_ADD_DSN");

            if (!DisplayConfigureDsnWindow(hwndParent, config))
                return FALSE;

            if (!RegisterDsn(config, driver))
                return FALSE;

            break;
        }

        case ODBC_CONFIG_DSN:
        {
            LOG_MSG("ODBC_CONFIG_DSN");

            std::string dsn = config.GetDsn();

            Configuration loaded(config);

            ReadDsnConfiguration(dsn.c_str(), loaded);

            if (!DisplayConfigureDsnWindow(hwndParent, loaded))
                return FALSE;

            if (!RegisterDsn(loaded, driver))
                return FALSE;

            if (loaded.GetDsn() != dsn && !UnregisterDsn(dsn.c_str()))
                return FALSE;

            break;
        }

        case ODBC_REMOVE_DSN:
        {
            LOG_MSG("ODBC_REMOVE_DSN");

            if (!UnregisterDsn(config.GetDsn().c_str()))
                return FALSE;

            break;
        }

        default:
            return FALSE;
    }

    return TRUE;
}