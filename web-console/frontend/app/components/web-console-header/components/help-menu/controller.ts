

import {service as GettingStartedServiceFactory} from 'app/modules/getting-started/GettingStarted.provider';

export default class HelpMenu {
    static $inject = ['gettingStarted'];

    constructor(private gettingStarted: ReturnType<typeof GettingStartedServiceFactory>) {}

    items = [
        {text: 'Getting Started', click: '$ctrl.gettingStarted.tryShow(true)'},
        {text: 'Documentation', href: 'https://docs.gridgain.com/docs/web-console', target: '_blank'},
        {text: 'Forums', href: 'https://forums.gridgain.com/home', target: '_blank'},
        {text: 'Support', href: 'https://gridgain.freshdesk.com/support/login', target: '_blank'},        
        {text: 'Whitepapers', href: 'https://www.gridgain.com/resources/literature/white-papers?combine=web+console&field_personas_target_id=All', target: '_blank'}
    ];
}
