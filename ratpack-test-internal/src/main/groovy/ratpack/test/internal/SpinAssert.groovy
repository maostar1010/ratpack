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

package ratpack.test.internal

import org.spockframework.lang.ConditionBlock
import spock.util.concurrent.PollingConditions

import java.time.Duration

@SuppressWarnings("GrMethodMayBeStatic")
class SpinAssert {

  private static final PollingConditions POLL = new PollingConditions(timeout: 25)

  @ConditionBlock
  final void of(Closure<?> closure) {
    POLL.eventually(closure)
  }

  @ConditionBlock
  final void of(int seconds, Closure<?> closure) {
    POLL.within(seconds, closure)
  }

  @ConditionBlock
  final void of(Duration timeout, Duration interval, Closure<?> closure) {
    println("Waiting for timeout of ${timeout}, checking at intervals of ${interval}.")
    def p = new PollingConditions()
    p.setDelay(interval.seconds.toDouble())
    p.setTimeout(timeout.seconds.toDouble())
    p.eventually(closure)
  }

}
