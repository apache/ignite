/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Component, Input} from '@angular/core';

@Component({
    selector: 'password-visibility-toggle-button-angular',
    templateUrl: `./passwordVisibilityToggleButton.template.html`,
    styleUrls: ['./passwordVisibilityToggleButton.style.url.scss']
})
export class PasswordVisibilityToggleButton {
    @Input()
    passwordEl: HTMLInputElement;

    isVisible: boolean = false;

    toggleVisibility() {
        this.isVisible = !this.isVisible;
        this.passwordEl.setAttribute('type', this.isVisible ? 'text' : 'password');
    }
}
