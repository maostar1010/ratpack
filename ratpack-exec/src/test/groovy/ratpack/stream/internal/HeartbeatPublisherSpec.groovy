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

package ratpack.stream.internal

import com.google.common.collect.ImmutableSet
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import ratpack.exec.BaseExecutionSpec
import ratpack.exec.Execution
import ratpack.exec.Promise
import ratpack.stream.Streams

import java.time.Duration

class HeartbeatPublisherSpec extends BaseExecutionSpec {

  def "can publish heartbeats"() {
    given:
    def source = Streams.periodically(execHarness.controller.getEventLoopGroup(), Duration.ofMillis(100)) {
      it < 5 ? it.toString() : null
    }.bindExec()

    def publisher = source.heartbeats(Duration.ofMillis(50), {}, Promise.value("heartbeat"))
    def subscriber = new TestSubscriber<String>(1)

    when:
    execHarness.controller.fork().start {
      subscriber.handler { val, subscription ->
        Execution.sleep(Duration.ofMillis(30)).then { subscription.request(1) }
      }
      publisher.subscribe(subscriber)
    }

    then:
    with(subscriber.result()) {
      containsAll((0..4)*.toString())
      count("heartbeat") > 2
    }
  }

  def "no heartbeats if source is fast"() {
    given:
    def source = Streams.periodically(execHarness.controller.getEventLoopGroup(), Duration.ofMillis(10)) {
      it < 5 ? it.toString() : null
    }.bindExec()

    def publisher = source.heartbeats(Duration.ofMillis(100), {}, Promise.value("heartbeat"))
    def subscriber = new TestSubscriber<String>()

    when:
    execHarness.controller.fork().start {
      publisher.subscribe(subscriber)
    }

    then:
    subscriber.result() == (0..4)*.toString()
  }

  def "all heartbeats if source is slow"() {
    given:
    def source = new Publisher() {
      @Override
      void subscribe(Subscriber s) {
        s.onSubscribe(new Subscription() {
          @Override
          void request(long n) {

          }

          @Override
          void cancel() {

          }
        })
      }
    }.bindExec()

    def publisher = source.heartbeats(Duration.ofMillis(10), {}, Promise.value("heartbeat"))
    def subscriber = new TestSubscriber<String>()

    when:
    execHarness.controller.fork().start {
      publisher.subscribe(subscriber)
    }

    spinAssert.of { subscriber.items().count("heartbeat") >= 5 }
    subscriber.subscription().cancel()

    then:
    subscriber.items().toSet() == ImmutableSet.of("heartbeat")
  }

}
