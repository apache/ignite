

import browserUpdate from 'browser-update';
import './style.scss';

browserUpdate({
    notify: {
        i: 11,
        f: '-18m',
        s: 9,
        c: '-18m',
        o: '-18m',
        e: '-6m'
    },
    l: 'en',
    mobile: false,
    api: 5,
    // This should work in older browsers
    text: '<b>Outdated or unsupported browser detected.</b> Web Console may work incorrectly. Please update to one of modern fully supported browsers! <a {up_but}>Update</a> <a {ignore_but}>Ignore</a>',
    reminder: 0
});
