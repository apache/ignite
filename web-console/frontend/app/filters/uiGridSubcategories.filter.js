

import _ from 'lodash';

export default () => {
    return (arr, category) => {
        return _.filter(arr, (item) => {
            return item.colDef.categoryDisplayName === category;
        });
    };
};
