function DefaultState($stateProvider) {
    const stateName = 'default-state';

    $stateProvider.state(stateName, {});

    return {
        setRedirectTo(fn) {
            const state = $stateProvider.stateRegistry.get(stateName);
            state.redirectTo = fn(state.redirectTo);
        },
        $get() {
            return this;
        }
    };
}

DefaultState.$inject = ['$stateProvider'];

export default DefaultState;
