

import {service as GettingStartedServiceFactory} from 'app/modules/getting-started/GettingStarted.provider';

export default class HelpMenu {
    static $inject = ['gettingStarted'];

    constructor(private gettingStarted: ReturnType<typeof GettingStartedServiceFactory>) {}

    items = [
        {text: 'Getting Started', click: '$ctrl.gettingStarted.tryShow(true)'},
        {text: 'Documentation', href: '/docs/web-console', target: '_blank'},
        {text: 'Forums', href: '/forum#ignite', target: '_blank'},
        {text: 'Support', href: '/support/login', target: '_blank'},        
        {text: 'Whitepapers', href: '/white-papers?combine=web+console&field_personas_target_id=All', target: '_blank'}
    ];
}
