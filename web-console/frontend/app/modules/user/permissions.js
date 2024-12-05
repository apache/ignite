

const guest = ['login'];
const becomed = ['profile', 'configuration'];
const user = becomed.concat(['logout', 'query', 'demo']);
const admin = user.concat(['admin_page']);

export default {
    guest,
    user,
    admin,
    becomed
};
