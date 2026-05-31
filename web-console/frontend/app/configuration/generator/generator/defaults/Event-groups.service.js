

import _ from 'lodash';

// Events groups.
import EVENT_GROUPS from 'app/data/event-groups.json';

export default class IgniteEventGroups {
    constructor() {
        return _.clone(EVENT_GROUPS);
    }
}
