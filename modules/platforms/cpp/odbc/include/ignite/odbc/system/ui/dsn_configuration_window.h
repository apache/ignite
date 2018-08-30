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

#ifndef _IGNITE_ODBC_SYSTEM_UI_DSN_CONFIGURATION_WINDOW
#define _IGNITE_ODBC_SYSTEM_UI_DSN_CONFIGURATION_WINDOW

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/system/ui/custom_window.h"

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                /**
                 * DSN configuration window class.
                 */
                class DsnConfigurationWindow : public CustomWindow
                {
                    /**
                     * Children windows ids.
                     */
                    struct ChildId
                    {
                        enum Type
                        {
                            CONNECTION_SETTINGS_GROUP_BOX = 100,
                            SSL_SETTINGS_GROUP_BOX,
                            ADDITIONAL_SETTINGS_GROUP_BOX,
                            AUTH_SETTINGS_GROUP_BOX,
                            NAME_EDIT,
                            NAME_LABEL,
                            ADDRESS_EDIT,
                            ADDRESS_LABEL,
                            SCHEMA_EDIT,
                            SCHEMA_LABEL,
                            PAGE_SIZE_EDIT,
                            PAGE_SIZE_LABEL,
                            DISTRIBUTED_JOINS_CHECK_BOX,
                            ENFORCE_JOIN_ORDER_CHECK_BOX,
                            REPLICATED_ONLY_CHECK_BOX,
                            COLLOCATED_CHECK_BOX,
                            LAZY_CHECK_BOX,
                            SKIP_REDUCER_ON_UPDATE_CHECK_BOX,
                            PROTOCOL_VERSION_LABEL,
                            PROTOCOL_VERSION_COMBO_BOX,
                            NESTED_TX_MODE_LABEL,
                            NESTED_TX_MODE_COMBO_BOX,
                            SSL_MODE_LABEL,
                            SSL_MODE_COMBO_BOX,
                            SSL_KEY_FILE_LABEL,
                            SSL_KEY_FILE_EDIT,
                            SSL_CERT_FILE_LABEL,
                            SSL_CERT_FILE_EDIT,
                            SSL_CA_FILE_LABEL,
                            SSL_CA_FILE_EDIT,
                            USER_LABEL,
                            USER_EDIT,
                            PASSWORD_LABEL,
                            PASSWORD_EDIT,
                            OK_BUTTON,
                            CANCEL_BUTTON
                        };
                    };

                    // Window margin size.
                    enum { MARGIN = 10 };

                    // Standard interval between UI elements.
                    enum { INTERVAL = 10 };

                    // Standard row height.
                    enum { ROW_HEIGHT = 20 };

                    // Standard button width.
                    enum { BUTTON_WIDTH = 80 };

                    // Standard button height.
                    enum { BUTTON_HEIGHT = 25 };

                public:
                    /**
                     * Constructor.
                     *
                     * @param parent Parent window handle.
                     */
                    explicit DsnConfigurationWindow(Window* parent, config::Configuration& config);

                    /**
                     * Destructor.
                     */
                    virtual ~DsnConfigurationWindow();

                    /**
                     * Create window in the center of the parent window.
                     */
                    void Create();

                    /**
                    * @copedoc ignite::odbc::system::ui::CustomWindow::OnCreate
                    */
                    virtual void OnCreate();

                    /**
                     * @copedoc ignite::odbc::system::ui::CustomWindow::OnMessage
                     */
                    virtual bool OnMessage(UINT msg, WPARAM wParam, LPARAM lParam);

                private:
                    IGNITE_NO_COPY_ASSIGNMENT(DsnConfigurationWindow)

                    /**
                     * Retrieves current values from the children and stores
                     * them to the specified configuration.
                     *
                     * @param cfg Configuration.
                     */
                    void RetrieveParameters(config::Configuration& cfg) const;

                    /**
                     * Retrieves current values from the connection UI group and
                     * stores them to the specified configuration.
                     *
                     * @param cfg Configuration.
                     */
                    void RetrieveConnectionParameters(config::Configuration& cfg) const;

                    /**
                     * Retrieves current values from the Authentication UI group and
                     * stores them to the specified configuration.
                     *
                     * @param cfg Configuration.
                     */
                    void RetrieveAuthParameters(config::Configuration& cfg) const;

                    /**
                     * Retrieves current values from the SSL UI group and
                     * stores them to the specified configuration.
                     *
                     * @param cfg Configuration.
                     */
                    void RetrieveSslParameters(config::Configuration& cfg) const;

                    /**
                     * Retrieves current values from the additional UI group and
                     * stores them to the specified configuration.
                     *
                     * @param cfg Configuration.
                     */
                    void RetrieveAdditionalParameters(config::Configuration& cfg) const;

                    /**
                     * Create connection settings group box.
                     *
                     * @param posX X position.
                     * @param posY Y position.
                     * @param sizeX Width.
                     * @return Size by Y.
                     */
                    int CreateConnectionSettingsGroup(int posX, int posY, int sizeX);

                    /**
                     * Create aythentication settings group box.
                     *
                     * @param posX X position.
                     * @param posY Y position.
                     * @param sizeX Width.
                     * @return Size by Y.
                     */
                    int CreateAuthSettingsGroup(int posX, int posY, int sizeX);

                    /**
                     * Create SSL settings group box.
                     *
                     * @param posX X position.
                     * @param posY Y position.
                     * @param sizeX Width.
                     * @return Size by Y.
                     */
                    int CreateSslSettingsGroup(int posX, int posY, int sizeX);

                    /**
                     * Create additional settings group box.
                     *
                     * @param posX X position.
                     * @param posY Y position.
                     * @param sizeX Width.
                     * @return Size by Y.
                     */
                    int CreateAdditionalSettingsGroup(int posX, int posY, int sizeX);

                    /** Window width. */
                    int width;

                    /** Window height. */
                    int height;

                    /** Connection settings group box. */
                    std::auto_ptr<Window> connectionSettingsGroupBox;

                    /** SSL settings group box. */
                    std::auto_ptr<Window> sslSettingsGroupBox;

                    /** Authentication settings group box. */
                    std::auto_ptr<Window> authSettingsGroupBox;

                    /** Additional settings group box. */
                    std::auto_ptr<Window> additionalSettingsGroupBox;

                    /** DSN name edit field label. */
                    std::auto_ptr<Window> nameLabel;

                    /** DSN name edit field. */
                    std::auto_ptr<Window> nameEdit;

                    /** DSN address edit field label. */
                    std::auto_ptr<Window> addressLabel;

                    /** DSN address edit field. */
                    std::auto_ptr<Window> addressEdit;

                    /** DSN schema edit field label. */
                    std::auto_ptr<Window> schemaLabel;

                    /** DSN schema edit field. */
                    std::auto_ptr<Window> schemaEdit;

                    /** DSN fetch page size edit field label. */
                    std::auto_ptr<Window> pageSizeLabel;

                    /** DSN fetch page size edit field. */
                    std::auto_ptr<Window> pageSizeEdit;

                    /** Distributed joins CheckBox. */
                    std::auto_ptr<Window> distributedJoinsCheckBox;

                    /** Enforce join order CheckBox. */
                    std::auto_ptr<Window> enforceJoinOrderCheckBox;

                    /** Replicated only CheckBox. */
                    std::auto_ptr<Window> replicatedOnlyCheckBox;

                    /** Collocated CheckBox. */
                    std::auto_ptr<Window> collocatedCheckBox;

                    /** Lazy CheckBox. */
                    std::auto_ptr<Window> lazyCheckBox;

                    /** Update on server CheckBox. */
                    std::auto_ptr<Window> skipReducerOnUpdateCheckBox;

                    /** Protocol version edit field. */
                    std::auto_ptr<Window> protocolVersionLabel;

                    /** Protocol verion ComboBox. */
                    std::auto_ptr<Window> protocolVersionComboBox;

                    /** Ok button. */
                    std::auto_ptr<Window> okButton;

                    /** Cancel button. */
                    std::auto_ptr<Window> cancelButton;

                    /** SSL Mode label. */
                    std::auto_ptr<Window> sslModeLabel;

                    /** SSL Mode ComboBox. */
                    std::auto_ptr<Window> sslModeComboBox;

                    /** SSL Private Key File label. */
                    std::auto_ptr<Window> sslKeyFileLabel;

                    /** SSL Private Key File edit. */
                    std::auto_ptr<Window> sslKeyFileEdit;

                    /** SSL Certificate File label. */
                    std::auto_ptr<Window> sslCertFileLabel;

                    /** SSL Certificate File edit. */
                    std::auto_ptr<Window> sslCertFileEdit;

                    /** SSL Certificate Authority File label. */
                    std::auto_ptr<Window> sslCaFileLabel;

                    /** SSL Certificate Authority File edit. */
                    std::auto_ptr<Window> sslCaFileEdit;

                    /** User label. */
                    std::auto_ptr<Window> userLabel;

                    /** User edit. */
                    std::auto_ptr<Window> userEdit;

                    /** Password label. */
                    std::auto_ptr<Window> passwordLabel;

                    /** Password edit. */
                    std::auto_ptr<Window> passwordEdit;

                    /** Nested transaction mode label. */
                    std::auto_ptr<Window> nestedTxModeLabel;

                    /** Nested transaction mode combo box. */
                    std::auto_ptr<Window> nestedTxModeComboBox;

                    /** Configuration. */
                    config::Configuration& config;

                    /** Flag indicating whether OK option was selected. */
                    bool accepted;
                };
            }
        }
    }
}

#endif //_IGNITE_ODBC_SYSTEM_UI_DSN_CONFIGURATION_WINDOW
