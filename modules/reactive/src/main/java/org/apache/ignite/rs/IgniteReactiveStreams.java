package org.apache.ignite.rs;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;

import java.io.Serializable;
import java.util.concurrent.Executors;

/**
 * RS Publisher adapter class
 * @param <R>
 */
public class IgniteReactiveStreams<R> implements Publisher<R>, Serializable {

    private Publisher<R> rsPublisher;

    public IgniteReactiveStreams(Publisher<R> publisher){
        this.rsPublisher = publisher;
    }

    @Override
    public void subscribe(Subscriber subscriber) {
        rsPublisher.subscribe(subscriber);
    }

    /** Utility function to concat multiple iterable as streams */
    public static <T> Publisher<T> iterablesToPublisher(Iterable<T>... iterables) {
        if(iterables == null || iterables.length == 0)
            return  null;
        Observable<T> observable = Observable.empty();
        for(Iterable<T> iterable : iterables){
            observable = observable.concatWith(Observable.create(new IterableStream(iterable)));
        }
        observable.subscribeOn(rx.schedulers.Schedulers.from(Executors.newFixedThreadPool(5))); //async
        return RxReactiveStreams.toPublisher(observable);
    }

    /**
     * Wrapper class to convert Iterable as Publisher
     * @param <T>
     */
    private static class IterableStream<T> implements Observable.OnSubscribe<T> {

        private Iterable<T> iterable;

        public IterableStream(Iterable<T> iterable){
            this.iterable = iterable;
        }

        @Override
        public void call(rx.Subscriber<? super T> subscriber) {
            for(T t : iterable){
                subscriber.onNext(t);
            }
        }
    }

}
