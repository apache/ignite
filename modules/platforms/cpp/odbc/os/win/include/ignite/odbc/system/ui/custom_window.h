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

#ifndef _IGNITE_ODBC_SYSTEM_UI_CUSTOM_WINDOW
#define _IGNITE_ODBC_SYSTEM_UI_CUSTOM_WINDOW

#include "ignite/odbc/system/ui/window.h"

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                /**
                 * Application execution result.
                 */
                struct Result
                {
                    enum Type
                    {
                        OK,
                        CANCEL
                    };
                };

                /**
                 * Process UI messages in current thread.
                 * Blocks until quit message has been received.
                 *
                 * @param window Main window.
                 * @return Application execution result.
                 */
                Result::Type ProcessMessages(Window& window);

                /**
                 * Window class.
                 */
                class CustomWindow : public Window
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param parent Parent window.
                     * @param className Window class name.
                     * @param title Window title.
                     */
                    CustomWindow(Window* parent, const char* className, const char* title);

                    /**
                     * Destructor.
                     */
                    virtual ~CustomWindow();

                    /**
                     * Callback which is called upon receiving new message.
                     * Pure virtual. Should be defined by user.
                     *
                     * @param msg Message.
                     * @param wParam Word-sized parameter.
                     * @param lParam Long parameter.
                     * @return Should return true if the message has been
                     *     processed by the handler and false otherwise.
                     */
                    virtual bool OnMessage(UINT msg, WPARAM wParam, LPARAM lParam) = 0;

                    /**
                     * Callback that is called upon window creation.
                     */
                    virtual void OnCreate() = 0;

                    /**
                     * Create child group box window.
                     *
                     * @param posX Position by X coordinate.
                     * @param posY Position by Y coordinate.
                     * @param sizeX Size by X coordinate.
                     * @param sizeY Size by Y coordinate.
                     * @param title Title.
                     * @param id ID to be assigned to the created window.
                     * @return Auto pointer containing new window.
                     */
                    std::auto_ptr<Window> CreateGroupBox(int posX, int posY,
                        int sizeX, int sizeY, const char* title, int id);

                    /**
                     * Create child label window.
                     *
                     * @param posX Position by X coordinate.
                     * @param posY Position by Y coordinate.
                     * @param sizeX Size by X coordinate.
                     * @param sizeY Size by Y coordinate.
                     * @param title Title.
                     * @param id ID to be assigned to the created window.
                     * @return Auto pointer containing new window.
                     */
                    std::auto_ptr<Window> CreateLabel(int posX, int posY,
                        int sizeX, int sizeY, const char* title, int id);

                    /**
                     * Create child Edit window.
                     *
                     * @param posX Position by X coordinate.
                     * @param posY Position by Y coordinate.
                     * @param sizeX Size by X coordinate.
                     * @param sizeY Size by Y coordinate.
                     * @param title Title.
                     * @param id ID to be assigned to the created window.
                     * @return Auto pointer containing new window.
                     */
                    std::auto_ptr<Window> CreateEdit(int posX, int posY,
                        int sizeX, int sizeY, const char* title, int id, int style = 0);

                    /**
                     * Create child button window.
                     *
                     * @param posX Position by X coordinate.
                     * @param posY Position by Y coordinate.
                     * @param sizeX Size by X coordinate.
                     * @param sizeY Size by Y coordinate.
                     * @param title Title.
                     * @param id ID to be assigned to the created window.
                     * @return Auto pointer containing new window.
                     */
                    std::auto_ptr<Window> CreateButton(int posX, int posY,
                        int sizeX, int sizeY, const char* title, int id);

                    /**
                     * Create child CheckBox window.
                     *
                     * @param posX Position by X coordinate.
                     * @param posY Position by Y coordinate.
                     * @param sizeX Size by X coordinate.
                     * @param sizeY Size by Y coordinate.
                     * @param title Title.
                     * @param id ID to be assigned to the created window.
                     * @return Auto pointer containing new window.
                     */
                    std::auto_ptr<Window> CreateCheckBox(int posX, int posY,
                        int sizeX, int sizeY, const char* title, int id, bool state);

                    /**
                     * Create child ComboBox window.
                     *
                     * @param posX Position by X coordinate.
                     * @param posY Position by Y coordinate.
                     * @param sizeX Size by X coordinate.
                     * @param sizeY Size by Y coordinate.
                     * @param title Title.
                     * @param id ID to be assigned to the created window.
                     * @return Auto pointer containing new window.
                     */
                    std::auto_ptr<Window> CreateComboBox(int posX, int posY,
                        int sizeX, int sizeY, const char* title, int id);
                private:
                    IGNITE_NO_COPY_ASSIGNMENT(CustomWindow)

                    /**
                     * Static callback.
                     *
                     * @param hwnd Window handle.
                     * @param msg Message.
                     * @param wParam Word-sized parameter.
                     * @param lParam Long parameter.
                     * @return Operation result.
                     */
                    static LRESULT CALLBACK WndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
                };
            }
        }
    }
}

#endif //_IGNITE_ODBC_SYSTEM_UI_CUSTOM_WINDOW