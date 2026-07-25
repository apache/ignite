

import {PageProfile} from '.';
import { TranslateService } from '@ngx-translate/core';
import { map, first } from 'rxjs/operators';
import { concat, pipe } from 'rxjs';

const getFirstAndMap = (fn) => pipe(first(), map(fn));

export const states = (translate: TranslateService) => concat(
    translate.get('profile.documentTitle').pipe(
        getFirstAndMap((title) => ({
            name: 'base.settings.profile',
            url: '/profile',
            component: PageProfile,
            permission: 'profile',
            tfMetaTags: {title}
        }))
    )
);
