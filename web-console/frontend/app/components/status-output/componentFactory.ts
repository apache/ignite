

import {StatusOptions} from './index';
import {Status} from './controller';
import {component} from './component';

export const componentFactory = (options: StatusOptions) => ({
    ...component,
    bindings: {
        value: '<'
    },
    controller: class extends Status {
        options = options
    }
});
