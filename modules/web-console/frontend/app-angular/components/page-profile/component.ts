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

import omit from 'lodash/fp/omit';
import {merge} from 'rxjs';
import {tap, filter} from 'rxjs/operators';
import {Component, Inject, OnInit, OnDestroy} from '@angular/core';
import {FormGroup, FormControl, Validators, FormBuilder} from '@angular/forms';
import templateUrl from 'file-loader!./template.html';
import {default as CountriesFactory, Country} from 'app/services/Countries.service';
import {default as UserFactory, User} from 'app/modules/user/User.service';
import {Confirm} from 'app/services/Confirm.service';
import {default as LegacyUtilsFactory} from 'app/services/LegacyUtils.service';
import {
    FORM_FIELD_OPTIONS, FormFieldRequiredMarkerStyles, FormFieldErrorStyles
} from '../form-field';
import './style.scss';

const passwordMatch = (newPassword: string) => (confirmPassword: FormControl) => newPassword === confirmPassword.value
    ? null
    : {passwordMatch: true};

const disableFormGroup = (fg: FormGroup) => {fg.disable(); return fg;};

@Component({
    selector: 'page-profile',
    templateUrl,
    viewProviders: [
        {
            provide: FORM_FIELD_OPTIONS,
            useValue: {
                requiredMarkerStyle: FormFieldRequiredMarkerStyles.OPTIONAL,
                errorStyle: FormFieldErrorStyles.ICON
            }
        }
    ]
})
export class PageProfile implements OnInit, OnDestroy {
    static parameters = [
        [new Inject('IgniteCountries')],
        [new Inject('User')],
        [new Inject('Confirm')],
        [new Inject('IgniteLegacyUtils')],
        [new Inject(FormBuilder)]
    ];

    constructor(
        Countries: ReturnType<typeof CountriesFactory>,
        private User: ReturnType<typeof UserFactory>,
        private Confirm: Confirm,
        private LegacyUtils: ReturnType<typeof LegacyUtilsFactory>,
        private fb: FormBuilder
    ) {
        this.countries = Countries.getAll();
    }

    countries: Country[];
    user: User;
    isLoading: boolean = false;

    async ngOnInit() {
        this.user = await this.User.read();
        this.form.patchValue(this.user);
    }

    ngOnDestroy() {
        this.subscriber.unsubscribe();
    }

    async saveUser(): Promise<void> {
        if (this.form.invalid) return;
        this.isLoading = true;
        try {
            const user = await this.User.save(this.prepareFormValue(this.form));
            this.form.patchValue(user);
            this.form.get('passwordPanelOpened').setValue(false);
        } finally {
            this.isLoading = false;
        }
    }

    prepareFormValue(form: PageProfile['form']): Partial<User> {
        return {
            ...omit(['password', 'passwordPanelOpened'])(form.value),
            token: form.controls.token.value,
            ...form.value.passwordPanelOpened ? {password: form.value.password.new} : {}
        };
    }

    async generateToken() {
        try {
            await this.Confirm.confirm('Are you sure you want to change security token?<br>If you change the token you will need to restart the agent.');
            this.form.get('token').setValue(this.LegacyUtils.randomString(20));
        }
        catch (ignored) {
            // No-op.
        }
    }

    form = this.fb.group({
        firstName: ['', Validators.required],
        lastName: ['', Validators.required],
        email: ['', [Validators.required, Validators.email]],
        phone: '',
        country: ['', Validators.required],
        company: ['', Validators.required],
        password: disableFormGroup(this.fb.group({
            new: ['', Validators.required],
            confirm: ''
        })),
        tokenPanelOpened: false,
        passwordPanelOpened: false,
        token: this.fb.control({value: '', disabled: true}, [Validators.required])
    });

    subscriber = merge(
        this.form.get('passwordPanelOpened').valueChanges.pipe(
            tap((opened: boolean) => {
                this.form.get('password')[opened ? 'enable' : 'disable']();
                this.form.get('password').updateValueAndValidity();
                if (opened) this.form.get('password').reset();
            })
        ),
        this.form.get('password.new').valueChanges.pipe(
            tap((newPassword: string) => {
                this.form.get('password.confirm').setValidators([Validators.required, passwordMatch(newPassword)]);
                this.form.get('password.confirm').updateValueAndValidity();
            })
        ),
        this.form.get('tokenPanelOpened').valueChanges.pipe(
            filter((opened) => opened === false),
            tap(async() => {
                this.form.get('token').reset((await this.User.read()).token);
            })
        )
    ).subscribe();
}
