/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ratpack.stream.internal;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import ratpack.api.Nullable;
import ratpack.exec.*;
import ratpack.func.Action;
import ratpack.stream.TransformablePublisher;

import java.time.Duration;
import java.util.function.Consumer;

public final class HeartbeatPublisher<T> implements TransformablePublisher<T> {

  private final Publisher<T> source;
  private final Promise<? extends T> heartbeat;
  private final Duration heartbeatFrequency;
  private final Consumer<? super T> disposer;

  public HeartbeatPublisher(
      Publisher<T> source,
      Promise<? extends T> heartbeat,
      Duration heartbeatFrequency,
      Consumer<? super T> disposer
  ) {
    this.source = source;
    this.heartbeat = heartbeat;
    this.heartbeatFrequency = heartbeatFrequency;
    this.disposer = disposer;
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    ExecController execController = ExecController.require();
    Transfer<ExecResult<T>> transfer = Transfer.create();
    execController.fork()
        .start(execution ->
            source.subscribe(new Subscriber<T>() {

              @Nullable
              volatile Subscription sourceSubscription;

              @Override
              public void onSubscribe(Subscription subscription) {
                sourceSubscription = subscription;
                subscription.request(1);
              }

              @Override
              public void onNext(T t) {
                transfer.put(ExecResult.of(Result.success(t)))
                    .then(consumed -> {
                      Subscription sourceSubscription = this.sourceSubscription;
                      if (consumed) {
                        if (sourceSubscription != null) {
                          sourceSubscription.request(1);
                        }
                      } else {
                        disposer.accept(t);
                        if (sourceSubscription != null) {
                          sourceSubscription.cancel();
                        }
                      }
                    });
              }

              @Override
              public void onError(Throwable t) {
                transfer.put(ExecResult.of(Result.error(t)))
                    .then(Action.noop());
              }

              @Override
              public void onComplete() {
                transfer.put(ExecResult.complete())
                    .then(Action.noop());
              }
            })
        );

    subscriber.onSubscribe(new ManagedSubscription<T>(subscriber, disposer::accept) {

      private boolean getting;

      @Override
      protected void onRequest(long n) {
        if (!getting) {
          get();
        }
      }

      private void get() {
        getting = true;
        transfer.tryGet(heartbeatFrequency)
            .then(itemOpt -> {
              if (itemOpt.isPresent()) {
                ExecResult<T> item = itemOpt.get();
                if (item.isSuccess()) {
                  emitNext(item.getValue());
                } else if (item.isError()) {
                  emitError(item.getThrowable());
                } else if (item.isComplete()) {
                  emitComplete();
                }
                postGet();
              } else {
                heartbeat
                    .onError(e -> {
                      cancel();
                      emitError(e);
                    })
                    .then(heartbeatValue -> {
                      emitNext(heartbeatValue);
                      postGet();
                    });
              }
            });
      }

      private void postGet() {
        if (hasDemand()) {
          get();
        } else {
          getting = false;
        }
      }

      @Override
      protected void onCancel() {
        execController.fork()
            .start(transfer.close());
      }
    });
  }
}
