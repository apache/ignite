

import _ from 'lodash';

export default () => (arr, search) => {
    if (!(arr && arr.length) || !search)
        return arr;

    return _.filter(arr, ({ name }) => name.indexOf(search) >= 0);
};
