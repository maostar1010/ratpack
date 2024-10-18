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

package ratpack.exec

import ratpack.exec.util.ParallelBatch
import ratpack.test.exec.ExecHarness
import ratpack.test.internal.BaseRatpackSpec
import spock.lang.AutoCleanup
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue

class OperationThrottleSpec extends BaseRatpackSpec {

  @AutoCleanup
  ExecHarness execHarness = ExecHarness.harness()

  PollingConditions polling = new PollingConditions(timeout: 5)

  def "can use unlimited throttle"() {
    def t = Throttle.unlimited()
    def v = execHarness.yield {
      Operation.async { it.success(null) }
        .throttled(t)
        .map { "foo" }
    }

    expect:
    v.value == "foo"
  }


  def "can use throttle"() {
    def t = Throttle.ofSize(1)
    def v = execHarness.yield {
      Promise.async { it.success("foo") }
        .throttled(t)
        .map { "foo" }
    }

    expect:
    v.value == "foo"
  }

  def "can throttle operations"() {
    def q = new LinkedBlockingQueue<Downstream<? super Void>>()

    def throttleSize = 5
    def jobs = 1000
    def t = Throttle.ofSize(throttleSize)
    def e = new ConcurrentLinkedQueue<Result<Void>>()
    def latch = new CountDownLatch(jobs)

    when:
    jobs.times {
      execHarness.fork().onComplete { latch.countDown() }.start {
        def exec = it
        Operation.async { q << it }
          .throttled(t)
          .result {
            assert Execution.current().is(exec)
            e << it
          }
      }
    }

    then:
    polling.eventually {
      q.size() == t.size
      t.active == q.size()
      t.waiting == jobs - t.size
    }

    execHarness.fork().start { Blocking.get { q.take().success(null) } then {} }

    polling.eventually {
      q.size() == t.size
      t.active == q.size()
      t.waiting == jobs - t.size - 1
    }

    execHarness.fork().start { e2 ->
      def n = jobs - 2 - throttleSize
      n.times {
        Blocking.get { q.take() } then {
          it.success(null)
        }
      }
    }

    polling.eventually {
      q.size() == t.size
      t.active == t.size
      t.waiting == 1
    }

    (throttleSize + 1).times {
      q.take().success(null)
    }

    polling.eventually {
      q.size() == 0
      t.active == 0
      t.waiting == 0
    }
  }

  def "can throttle within same execution"() {
    when:
    def t = Throttle.ofSize(5)
    def l = []
    execHarness.run {
      6.times { i ->
        Operation.async { it.success(null) }
          .throttled(t)
          .map { i }
          .then { l << it }
      }
    }

    then:
    l == [0, 1, 2, 3, 4, 5]
  }

  def "throttled operations can be routed"() {
    given:
    Throttle throttle = Throttle.ofSize(2)
    List<Promise> promises = []

    for (int i = 0; i < 100; i++) {
      promises << Operation.of {}
        .transform { up ->
          return { down ->
            up.connect(down.onSuccess { down.complete() })
          } as Upstream<Void>
        }
        .throttled(throttle)
        .promise()
    }
    when:
    def results = ExecHarness.yieldSingle {
      ParallelBatch.of(promises).yieldAll()
    }.value

    then:
    results.every { result -> result.complete }
  }
}
