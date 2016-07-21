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

#include "ignite/odbc/system/ui/dsn_configuration_window.h"

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                DsnConfigurationWindow::DsnConfigurationWindow(Window* parent, config::Configuration& config):
                    CustomWindow(parent, "IgniteConfigureDsn", "Configure Apache Ignite DSN"),
                    width(320),
                    height(200),
                    nameLabel(),
                    nameEdit(),
                    serverLabel(),
                    serverEdit(),
                    portLabel(),
                    portEdit(),
                    cacheLabel(),
                    cacheEdit(),
                    okButton(),
                    cancelButton(),
                    config(config),
                    accepted(false)
                {
                    // No-op.
                }

                DsnConfigurationWindow::~DsnConfigurationWindow()
                {
                    // No-op.
                }

                void DsnConfigurationWindow::Create()
                {
                    // Finding out parent position.
                    RECT parentRect;
                    GetWindowRect(parent->GetHandle(), &parentRect);

                    // Positioning window to the center of parent window.
                    const int posX = parentRect.left + (parentRect.right - parentRect.left - width) / 2;
                    const int posY = parentRect.top + (parentRect.bottom - parentRect.top - height) / 2;

                    RECT desiredRect = {posX, posY, posX + width, posY + height};
                    AdjustWindowRect(&desiredRect, WS_BORDER | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME, FALSE);

                    Window::Create(WS_OVERLAPPED | WS_SYSMENU, desiredRect.left, desiredRect.top,
                        desiredRect.right - desiredRect.left, desiredRect.bottom - desiredRect.top, 0);

                    if (!handle)
                    {
                        std::stringstream buf;

                        buf << "Can not create window, error code: " << GetLastError();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
                    }
                }

                void DsnConfigurationWindow::OnCreate()
                {
                    int margin = 10;
                    int interval = 10;

                    int labelSizeX = 60;
                    int labelPosX = margin + interval;

                    int editSizeX = width - labelSizeX - 2 * margin - 3 * interval;
                    int editPosX = margin + labelSizeX + 2 * interval;

                    int rowSize = 20;
                    int rowPos = margin + 2 * interval;

                    const char* val = config.GetDsn().c_str();
                    nameLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "DSN name:", ID_NAME_LABEL);
                    nameEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ID_NAME_EDIT);

                    rowPos += interval + rowSize;

                    val = config.GetHost().c_str();
                    serverLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Server:", ID_SERVER_LABEL);
                    serverEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ID_SERVER_EDIT);

                    rowPos += interval + rowSize;

                    std::stringstream buf;
                    buf << config.GetTcpPort();
                    std::string strPort = buf.str();

                    val = strPort.c_str();
                    portLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Port:", ID_PORT_LABEL);
                    portEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ID_PORT_EDIT, ES_NUMBER);

                    rowPos += interval + rowSize;

                    val = config.GetCache().c_str();
                    cacheLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Cache:", ID_CACHE_LABEL);
                    cacheEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ID_CACHE_EDIT);

                    rowPos += interval * 2 + rowSize;
                    rowSize = 25;

                    connectionSettingsGroupBox = CreateGroupBox(margin, margin, width - 2 * margin,
                        rowPos - 2 * margin, "Connection settings", ID_CONNECTION_SETTINGS_GROUP_BOX);

                    int buttonSizeX = 80;
                    int cancelPosX = width - margin - buttonSizeX;
                    int okPosX = cancelPosX - interval - buttonSizeX;

                    okButton = CreateButton(okPosX, rowPos, buttonSizeX, rowSize, "Ok", ID_OK_BUTTON);
                    cancelButton = CreateButton(cancelPosX, rowPos, buttonSizeX, rowSize, "Cancel", ID_CANCEL_BUTTON);
                }

                bool DsnConfigurationWindow::OnMessage(UINT msg, WPARAM wParam, LPARAM lParam)
                {
                    switch (msg)
                    {
                        case WM_COMMAND:
                        {
                            switch (LOWORD(wParam))
                            {
                                case ID_OK_BUTTON:
                                {
                                    try
                                    {
                                        RetrieveParameters(config);

                                        accepted = true;

                                        PostMessage(GetHandle(), WM_CLOSE, 0, 0);
                                    }
                                    catch (IgniteError& err)
                                    {
                                        MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);
                                    }

                                    break;
                                }

                                case IDCANCEL:
                                case ID_CANCEL_BUTTON:
                                {
                                    PostMessage(GetHandle(), WM_CLOSE, 0, 0);

                                    break;
                                }

                                default:
                                    return false;
                            }

                            break;
                        }

                        case WM_DESTROY:
                        {
                            PostQuitMessage(accepted ? RESULT_OK : RESULT_CANCEL);

                            break;
                        }

                        default:
                            return false;
                    }

                    return true;
                }

                void DsnConfigurationWindow::RetrieveParameters(config::Configuration& cfg) const
                {
                    std::string dsn;
                    std::string server;
                    std::string port;
                    std::string cache;

                    nameEdit->GetText(dsn);
                    serverEdit->GetText(server);
                    portEdit->GetText(port);
                    cacheEdit->GetText(cache);

                    LOG_MSG("Retriving arguments:\n");
                    LOG_MSG("DSN:    %s\n", dsn.c_str());
                    LOG_MSG("Server: %s\n", server.c_str());
                    LOG_MSG("Port:   %s\n", port.c_str());
                    LOG_MSG("Cache:  %s\n", cache.c_str());

                    if (dsn.empty())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "DSN name can not be empty.");

                    int32_t numPort = common::LexicalCast<int32_t>(port);

                    if (port.size() > (sizeof("65535") - 1) || numPort <= 0 || numPort >= INT16_MAX)
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Port value should be in range from 1 to 65534.");

                    cfg.SetDsn(dsn);
                    cfg.SetHost(server);
                    cfg.SetTcpPort(numPort);
                    cfg.SetCache(cache);
                }
            }
        }
    }
}
