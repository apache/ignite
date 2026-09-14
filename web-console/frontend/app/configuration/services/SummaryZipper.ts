

import Worker from './summary.worker';

export default function SummaryZipperService($q: ng.IQService) {
    return function(message) {
        const defer = $q.defer();
        const worker = new Worker();

        worker.postMessage(message);

        worker.onmessage = (e) => {
            defer.resolve(e.data);
            worker.terminate();
        };

        worker.onerror = (err) => {
            defer.reject(new Error(err.message));
            worker.terminate();
        };

        return defer.promise;
    };
}

SummaryZipperService.$inject = ['$q'];
