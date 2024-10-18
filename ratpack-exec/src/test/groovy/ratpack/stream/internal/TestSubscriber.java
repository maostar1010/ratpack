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

import com.google.common.collect.ImmutableList;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import ratpack.api.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class TestSubscriber<T> implements Subscriber<T> {

  @Nullable
  private Subscription subscription;

  private final Queue<T> received = new ConcurrentLinkedQueue<>();
  private final Consumer<? super Subscription> subscriptionConsumer;

  private BiConsumer<T, Subscription> onNext = (item, subscription) -> {
  };

  private final CompletableFuture<List<T>> resultFuture = new CompletableFuture<>();

  public TestSubscriber(Consumer<? super Subscription> subscriptionConsumer) {
    this.subscriptionConsumer = subscriptionConsumer;
  }

  public TestSubscriber(long initialRequest) {
    this.subscriptionConsumer = subscription -> subscription.request(initialRequest);
  }

  public TestSubscriber() {
    this(Long.MAX_VALUE);
  }

  @Override
  public void onSubscribe(Subscription s) {
    subscription = s;
    subscriptionConsumer.accept(s);
  }

  @Override
  public void onNext(T t) {
    received.add(t);
    onNext.accept(t, subscription);
  }

  @Override
  public void onError(Throwable t) {
    resultFuture.completeExceptionally(t);
  }

  @Override
  public void onComplete() {
    resultFuture.complete(items());
  }

  public TestSubscriber<T> handler(BiConsumer<T, Subscription> onNext) {
    this.onNext = onNext;
    return this;
  }

  public Subscription subscription() {
    return Objects.requireNonNull(subscription);
  }

  public List<T> items() {
    return ImmutableList.copyOf(received);
  }

  public List<T> result() throws ExecutionException, InterruptedException {
    return resultFuture.get();
  }

}
