

// Filter that return 'true' if caches has at least one domain with 'generatePojo' flag.
export default () => ({caches} = []) =>
    _.find(caches, (cache) => cache.domains && cache.domains.length &&
        cache.domains.find((domain) => domain.generatePojo));
