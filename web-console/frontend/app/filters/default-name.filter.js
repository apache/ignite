

import _ from 'lodash';

export default () => {
    /**
     * Filter that will check name and return `<default>` if needed.
     * @param {string} name
     * @param {string} html
     */
    const filter = (name, html) => _.isEmpty(name) ? (html ? '&lt;default&gt;' : '<default>') : name;

    return filter;
};
