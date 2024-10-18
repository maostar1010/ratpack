/*
 * Copyright 2014 the original author or authors.
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

package ratpack.exec;

import ratpack.exec.internal.DefaultTransfer;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A utility for transferring a value between separate producer and consumer executions.
 * <p>
 * This construct is equivalent to {@link LinkedBlockingQueue}, but for a single value and is non-blocking.
 * <p>
 * It is important to {@link #close()} transfers when done to release any waiting producers or consumers.
 *
 * @since 1.10
 */
public interface Transfer<T> {

  /**
   * Creates a transfer.
   *
   * @return a transfer
   * @param <T> the transferred value type
   */
  static <T> Transfer<T> create() {
    return new DefaultTransfer<>();
  }

  /**
   * Writes the value for transfer.
   * <p>
   * The returned promise will complete with true when the value is consumed.
   * It will complete with false if the transfer is closed before the value is consumed.
   *
   * @param value the value to transfer
   * @return a promise indicating whether the value was transferred
   */
  Promise<Boolean> put(T value);

  /**
   * Consumes a transferred value, waiting up to the given timeout.
   * <p>
   * The returned promise will complete with an empty optional if the timeout elapses or if the transfer is closed.
   * It will complete with a non-empty optional if a value was transferred.
   *
   * @param timeout how long to wait for a value to be transferred
   * @return a promise for the transferred value
   */
  Promise<Optional<T>> tryGet(Duration timeout);

  /**
   * Consumes a transferred value.
   * <p>
   * The returned promise will complete with an empty optional if the transfer is closed.
   * It will complete with a non-empty optional if a value was transferred.
   *
   * @return a promise for the transferred value
   */
  default Promise<Optional<T>> get() {
    return tryGet(Duration.ZERO);
  }

  /**
   * Releases any waiting put/get operations.
   * <p>
   * After closure, all subsequent put/get operations return immediately.
   *
   * @return an operation that closes the transfer
   */
  Operation close();
}
