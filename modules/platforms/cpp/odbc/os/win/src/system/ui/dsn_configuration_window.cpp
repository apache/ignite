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
                    nameLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "DSN name:", ChildId::NAME_LABEL);
                    nameEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ChildId::NAME_EDIT);

                    rowPos += interval + rowSize;

                    val = config.GetAddress().c_str();
                    addressLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Address:", ChildId::ADDRESS_LABEL);
                    addressEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ChildId::ADDRESS_EDIT);

                    rowPos += interval + rowSize;

                    val = config.GetCache().c_str();
                    cacheLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize, "Cache name:", ChildId::CACHE_LABEL);
                    cacheEdit = CreateEdit(editPosX, rowPos, editSizeX, rowSize, val, ChildId::CACHE_EDIT);

                    rowPos += interval + rowSize;

                    std::string tmp = common::LexicalCast<std::string>(config.GetPageSize());
                    val = tmp.c_str();
                    pageSizeLabel = CreateLabel(labelPosX, rowPos, labelSizeX,
                        rowSize, "Page size:", ChildId::PAGE_SIZE_LABEL);

                    pageSizeEdit = CreateEdit(editPosX, rowPos, editSizeX, 
                        rowSize, val, ChildId::PAGE_SIZE_EDIT, ES_NUMBER);

                    rowPos += interval + rowSize;

                    protocolVersionLabel = CreateLabel(labelPosX, rowPos, labelSizeX, rowSize,
                        "Protocol version:", ChildId::PROTOCOL_VERSION_LABEL);
                    protocolVersionComboBox = CreateComboBox(editPosX, rowPos, editSizeX, rowSize,
                        "Protocol version", ChildId::PROTOCOL_VERSION_COMBO_BOX);

                    int id = 0;

                    const ProtocolVersion::VersionSet& supported = ProtocolVersion::GetSupported();

                    ProtocolVersion::VersionSet::const_iterator it;
                    for (it = supported.begin(); it != supported.end(); ++it)
                    {
                        protocolVersionComboBox->AddString(it->ToString());

                        if (*it == config.GetProtocolVersion())
                            protocolVersionComboBox->SetSelection(id);

                        ++id;
                    }

                    rowPos += interval + rowSize;

                    distributedJoinsCheckBox = CreateCheckBox(editPosX, rowPos, checkBoxSize, rowSize,
                        "Distributed Joins", ChildId::DISTRIBUTED_JOINS_CHECK_BOX, config.IsDistributedJoins());

                    enforceJoinOrderCheckBox = CreateCheckBox(editPosX + checkBoxSize + interval, rowPos, checkBoxSize,
                        rowSize, "Enforce Join Order", ChildId::ENFORCE_JOIN_ORDER_CHECK_BOX, config.IsEnforceJoinOrder());

                    rowPos += interval * 2 + rowSize;

                    connectionSettingsGroupBox = CreateGroupBox(margin, sectionBegin, width - 2 * margin,
                        rowPos - interval - sectionBegin, "Connection settings", ChildId::CONNECTION_SETTINGS_GROUP_BOX);

                    int buttonSizeX = 80;
                    int cancelPosX = width - margin - buttonSizeX;
                    int okPosX = cancelPosX - interval - buttonSizeX;

                    rowSize = 25;

                    okButton = CreateButton(okPosX, rowPos, buttonSizeX, rowSize, "Ok", ChildId::OK_BUTTON);
                    cancelButton = CreateButton(cancelPosX, rowPos, buttonSizeX, rowSize, "Cancel", ChildId::CANCEL_BUTTON);
                }

                bool DsnConfigurationWindow::OnMessage(UINT msg, WPARAM wParam, LPARAM lParam)
                {
                    switch (msg)
                    {
                        case WM_COMMAND:
                        {
                            switch (LOWORD(wParam))
                            {
                                case ChildId::OK_BUTTON:
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
                                case ChildId::CANCEL_BUTTON:
                                {
                                    PostMessage(GetHandle(), WM_CLOSE, 0, 0);

                                    break;
                                }

                                case ChildId::DISTRIBUTED_JOINS_CHECK_BOX:
                                {
                                    distributedJoinsCheckBox->SetChecked(!distributedJoinsCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::ENFORCE_JOIN_ORDER_CHECK_BOX:
                                {
                                    enforceJoinOrderCheckBox->SetChecked(!enforceJoinOrderCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::PROTOCOL_VERSION_COMBO_BOX:
                                default:
                                    return false;
                            }

                            break;
                        }

                        case WM_DESTROY:
                        {
                            PostQuitMessage(accepted ? Result::OK : Result::CANCEL);

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
