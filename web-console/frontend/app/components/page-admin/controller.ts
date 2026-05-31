

import UserNotificationsService from '../user-notifications/service';

export default class PageAdminCtrl {
    static $inject = ['UserNotifications'];

    constructor(private UserNotifications: UserNotificationsService) {}

    changeUserNotifications() {
        this.UserNotifications.editor();
    }
}
