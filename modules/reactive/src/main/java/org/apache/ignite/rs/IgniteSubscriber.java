package org.apache.ignite.rs;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.internal.reactivestreams.SubscriberAdapter;

import java.io.Serializable;

/**
 * Created by lalitj on 27/9/15.
 */
public class IgniteSubscriber<T> extends rx.Subscriber<T> implements Subscriber<T>, Serializable {

    private SubscriberAdapter<T> adapter;

    public IgniteSubscriber(){
        this.adapter = new SubscriberAdapter<T>(this);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }

    @Override
    public void onSubscribe(Subscription subscription) {
        adapter.onSubscribe(subscription);
    }

    @Override
    public void onNext(T t) {
        System.out.println(t);
    }

    @Override
    public void onCompleted() {
        this.onComplete();
    }
}
