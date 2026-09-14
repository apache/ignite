

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
