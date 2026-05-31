

import {default as Version} from '../../services/Version.service';
import {map} from 'rxjs/operators';
import {UserService} from '../../modules/user/User.service';

export default class WebConsoleFooter {
    static $inject = ['IgniteVersion', 'User'];

    constructor(private Version: Version, private User: UserService) {}

    year = new Date().getFullYear();

    userIsAuthorized$ = this.User.current$.pipe(map((user) => !!user))
}
