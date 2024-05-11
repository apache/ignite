

import {User} from '../../types';

export const USER: 'USER' = 'USER';
export const user = (user: User) => ({type: USER, user});

export type UserActions =
    | ReturnType<typeof user>;
