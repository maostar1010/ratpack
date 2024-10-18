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

package ratpack.exec.internal;

import ratpack.api.Nullable;
import ratpack.exec.*;
import ratpack.exec.util.ReadWriteAccess;
import ratpack.func.Action;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class DefaultTransfer<T> implements Transfer<T> {

  private final ReadWriteAccess lock = ReadWriteAccess.create(Duration.ZERO);
  private volatile @Nullable Exchange<T> exchange;

  interface Exchange<T> {

    void close();

  }

  @Override
  public Promise<Boolean> put(T value) {
    return Promise.async(downstream ->
      lock.write(Operation.of(() -> {
          Exchange<T> exchange1 = this.exchange;
          if (exchange1 == null) {
            this.exchange = new Writer<>(value, downstream);
          } else if (exchange1 instanceof DefaultTransfer.Closed) {
            downstream.success(false);
          } else if (exchange1 instanceof DefaultTransfer.Writer) {
            downstream.error(new IllegalStateException("overlapping Transfer.put() operation attempted with: " + value));
          } else if (exchange1 instanceof DefaultTransfer.Reader) {
            this.exchange = null;
            Reader<T> reader = (Reader<T>) exchange1;
            if (reader.timeoutFuture != null) {
              reader.timeoutFuture.cancel(true);
            }
            reader.downstream.success(Optional.of(value));
            downstream.success(true);
          } else {
            downstream.error(new IllegalStateException("unhandled exchange: " + exchange1));
          }
        }))
        .connect(downstream.onSuccess(Action.noop()))
    );
  }

  @Override
  public Promise<Optional<T>> tryGet(Duration timeout) {
    return Promise.async(downstream ->
      lock.write(Operation.of(() -> {
          Exchange<T> exchange1 = this.exchange;
          if (exchange1 == null) {
            Reader<T> reader = new Reader<>(downstream);
            this.exchange = reader;
            if (isPositive(timeout)) {
              reader.timeoutFuture = createTimeout(timeout, reader);
            }
          } else if (exchange1 instanceof DefaultTransfer.Closed) {
            downstream.success(Optional.empty());
          } else if (exchange1 instanceof DefaultTransfer.Writer) {
            this.exchange = null;
            Writer<T> writer = (Writer<T>) exchange1;
            writer.downstream.success(true);
            downstream.success(Optional.of(writer.item));
          } else {
            downstream.error(new IllegalStateException("unhandled exchange: " + exchange1));
          }
        }))
        .connect(downstream.onSuccess(Action.noop()))
    );
  }

  private static boolean isPositive(Duration timeout) {
    return (timeout.getSeconds() | timeout.getNano()) > 0;
  }

  private ScheduledFuture<?> createTimeout(Duration timeout, Reader<T> reader) {
    ExecController execController = ExecController.require();
    return execController
      .getEventLoopGroup()
      .schedule(
        () ->
          execController.fork()
            .start(lock.write(Operation.of(() -> {
              Exchange<T> exchange = this.exchange;
              if (exchange == reader) {
                this.exchange = null;
                reader.downstream.success(Optional.empty());
              }
            }))),
        timeout.toNanos(),
        TimeUnit.NANOSECONDS
      );
  }

  @Override
  public Operation close() {
    return lock.write(Operation.of(() -> {
      Exchange<T> existing = this.exchange;
      this.exchange = Closed.instance();
      if (existing != null) {
        existing.close();
      }
    }));
  }

  private static final class Reader<T> implements Exchange<T> {
    private final ratpack.exec.Downstream<? super Optional<T>> downstream;
    private ScheduledFuture<?> timeoutFuture;

    public Reader(Downstream<? super Optional<T>> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void close() {
      downstream.success(Optional.empty());
    }
  }

  private static final class Writer<T> implements DefaultTransfer.Exchange<T> {
    private final T item;
    private final Downstream<? super Boolean> downstream;

    Writer(T item, Downstream<? super Boolean> downstream) {
      this.item = item;
      this.downstream = downstream;
    }

    @Override
    public void close() {
      downstream.success(false);
    }
  }

  private static final class Closed<T> implements Exchange<T> {
    static final Closed<Object> INSTANCE = new Closed<>();

    @SuppressWarnings("unchecked")
    static <T> Closed<T> instance() {
      return (Closed<T>) INSTANCE;
    }

    @Override
    public void close() {

    }
  }

}
