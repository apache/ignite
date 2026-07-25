

import {component} from './component';
export {componentFactory} from './componentFactory';

export interface StatusOption {
    level: StatusLevel,
    // Whether to show progress indicator or not
    ongoing?: boolean,
    value: string | boolean,
    // What user will see
    label: string
}

export type StatusOptions = Array<StatusOption>;

export enum StatusLevel {
    NEUTRAL = 'NEUTRAL',
    GREEN = 'GREEN',
    RED = 'RED'
}

export default angular
    .module('ignite-console.components.status-output', [])
    .component('statusOutput', component);
