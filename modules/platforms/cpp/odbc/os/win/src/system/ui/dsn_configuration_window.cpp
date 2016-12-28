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

#include "ignite/odbc/log.h"

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
                    width(360),
                    height(270),
                    connectionSettingsGroupBox(),
                    nameLabel(),
                    nameEdit(),
                    addressLabel(),
                    addressEdit(),
                    cacheLabel(),
                    cacheEdit(),
                    pageSizeLabel(),
                    pageSizeEdit(),
                    distributedJoinsCheckBox(),
                    enforceJoinOrderCheckBox(),
                    protocolVersionLabel(),
                    protocolVersionComboBox(),
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

                    int labelSizeX = 80;
                    int labelPosX = margin + interval;

                    int editSizeX = width - labelSizeX - 2 * margin - 3 * interval;
                    int editPosX = margin + labelSizeX + 2 * interval;

                    int rowSize = 20;
                    int rowPos = margin + 2 * interval;

                    int checkBoxSize = (editSizeX - interval) / 2;

                    int sectionBegin = margin;

                    const char* val = config.GetDsn().c_str();
                    nameLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "DSN name:", ID_NAME_LABEL);
                    nameEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ID_NAME_EDIT);

                    rowPos += interval + rowSize;

                    val = config.GetAddress().c_str();
                    addressLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Address:", ID_ADDRESS_LABEL);
                    addressEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ID_ADDRESS_EDIT);

                    rowPos += interval + rowSize;

                    val = config.GetCache().c_str();
                    cacheLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Cache name:", ID_CACHE_LABEL);
                    cacheEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ID_CACHE_EDIT);

                    rowPos += interval + rowSize;

                    std::string tmp = common::LexicalCast<std::string>(config.GetPageSize());
                    val = tmp.c_str();
                    pageSizeLabel = CreateLabel(labelPosX, rowPos, labelSizeX,
                        rowSize, "Page size:", ID_PAGE_SIZE_LABEL);

                    pageSizeEdit = CreateEdit(editPosX, rowPos, editSizeX, 
                        rowSize, val, ID_PAGE_SIZE_EDIT, ES_NUMBER);

                    rowPos += interval + rowSize;

                    protocolVersionLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize,
                        "Protocol version:", ID_PROTOCOL_VERSION_LABEL);
                    protocolVersionComboBox = CreateComboBox(editPosX, rowPos, editSizeX, rowSize,
                        "Protocol version", ID_PROTOCOL_VERSION_COMBO_BOX);

                    int id = 0;

                    const ProtocolVersion::StringToVersionMap& versionMap = ProtocolVersion::GetMap();

                    ProtocolVersion::StringToVersionMap::const_iterator it;
                    for (it = versionMap.begin(); it != versionMap.end(); ++it)
                    {
                        protocolVersionComboBox->AddString(it->first);

                        if (it->second == config.GetProtocolVersion())
                            protocolVersionComboBox->SetSelection(id);

                        ++id;
                    }

                    rowPos += interval + rowSize;

                    distributedJoinsCheckBox = CreateCheckBox(editPosX, rowPos, checkBoxSize, rowSize,
                        "Distributed Joins", ID_DISTRIBUTED_JOINS_CHECK_BOX, config.IsDistributedJoins());

                    enforceJoinOrderCheckBox = CreateCheckBox(editPosX + checkBoxSize + interval, rowPos, checkBoxSize,
                        rowSize, "Enforce Join Order", ID_ENFORCE_JOIN_ORDER_CHECK_BOX, config.IsEnforceJoinOrder());

                    if (!config.GetProtocolVersion().IsDistributedJoinsSupported())
                    {
                        distributedJoinsCheckBox->SetEnabled(false);
                        enforceJoinOrderCheckBox->SetEnabled(false);
                    }

                    rowPos += interval * 2 + rowSize;

                    connectionSettingsGroupBox = CreateGroupBox(margin, sectionBegin, width - 2 * margin,
                        rowPos - interval - sectionBegin, "Connection settings", ID_CONNECTION_SETTINGS_GROUP_BOX);

                    int buttonSizeX = 80;
                    int cancelPosX = width - margin - buttonSizeX;
                    int okPosX = cancelPosX - interval - buttonSizeX;

                    rowSize = 25;

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

                                case ID_PROTOCOL_VERSION_COMBO_BOX:
                                {
                                    if (HIWORD(wParam) == CBN_SELCHANGE)
                                    {
                                        std::string text;

                                        protocolVersionComboBox->GetText(text);

                                        ProtocolVersion version = ProtocolVersion::FromString(text);

                                        if (!version.IsUnknown() && !version.IsDistributedJoinsSupported())
                                        {
                                            distributedJoinsCheckBox->SetEnabled(false);
                                            enforceJoinOrderCheckBox->SetEnabled(false);
                                        }
                                        else
                                        {
                                            distributedJoinsCheckBox->SetEnabled(true);
                                            enforceJoinOrderCheckBox->SetEnabled(true);
                                        }
                                    }

                                    break;
                                }

                                case IDCANCEL:
                                case ID_CANCEL_BUTTON:
                                {
                                    PostMessage(GetHandle(), WM_CLOSE, 0, 0);

                                    break;
                                }

                                case ID_DISTRIBUTED_JOINS_CHECK_BOX:
                                {
                                    distributedJoinsCheckBox->SetChecked(!distributedJoinsCheckBox->IsChecked());

                                    break;
                                }

                                case ID_ENFORCE_JOIN_ORDER_CHECK_BOX:
                                {
                                    enforceJoinOrderCheckBox->SetChecked(!enforceJoinOrderCheckBox->IsChecked());

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
                    std::string address;
                    std::string cache;
                    std::string pageSizeStr;
                    std::string version;

                    bool distributedJoins;
                    bool enforceJoinOrder;

                    nameEdit->GetText(dsn);
                    addressEdit->GetText(address);
                    cacheEdit->GetText(cache);
                    protocolVersionComboBox->GetText(version);
                    pageSizeEdit->GetText(pageSizeStr);

                    int32_t pageSize = common::LexicalCast<int32_t>(pageSizeStr);

                    if (pageSize <= 0)
                        pageSize = config.GetPageSize();

                    common::StripSurroundingWhitespaces(address);
                    common::StripSurroundingWhitespaces(dsn);

                    distributedJoins = distributedJoinsCheckBox->IsEnabled() && distributedJoinsCheckBox->IsChecked();
                    enforceJoinOrder = enforceJoinOrderCheckBox->IsEnabled() && enforceJoinOrderCheckBox->IsChecked();

                    LOG_MSG("Retriving arguments:");
                    LOG_MSG("DSN:                " << dsn);
                    LOG_MSG("Address:            " << address);
                    LOG_MSG("Cache:              " << cache);
                    LOG_MSG("Page size:          " << pageSize);
                    LOG_MSG("Protocol version:   " << version);
                    LOG_MSG("Distributed Joins:  " << (distributedJoins ? "true" : "false"));
                    LOG_MSG("Enforce Join Order: " << (enforceJoinOrder ? "true" : "false"));

                    if (dsn.empty())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "DSN name can not be empty.");

                    cfg.SetDsn(dsn);
                    cfg.SetAddress(address);
                    cfg.SetCache(cache);
                    cfg.SetPageSize(pageSize);
                    cfg.SetProtocolVersion(version);
                    cfg.SetDistributedJoins(distributedJoins);
                    cfg.SetEnforceJoinOrder(enforceJoinOrder);
                }
            }
        }
    }
}
