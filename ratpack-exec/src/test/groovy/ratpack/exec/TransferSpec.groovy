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

import ratpack.exec.util.SerialBatch
import ratpack.test.exec.ExecHarness
import ratpack.test.internal.BaseRatpackSpec
import spock.lang.AutoCleanup
import spock.util.concurrent.BlockingVariable

import java.time.Duration

class TransferSpec extends BaseRatpackSpec {

  @AutoCleanup
  ExecHarness harness = ExecHarness.harness()

  def transfer = Transfer.<Integer> create()

  void cleanup() {
    harness.execute(transfer.close())
  }

  def "can transfer items"() {
    when:
    Thread.start {
      harness.yield {
        SerialBatch.of((1..10).collect { transfer.put(it) })
          .yield()
      }
    }

    then:
    def results = harness.yield {
      SerialBatch.of((1..10).collect { transfer.get() })
        .yield()
    }.valueOrThrow

    results*.get() == (1..10).asList()
  }

  def "can timeout read"() {
    when:
    Thread.start { harness.yield { transfer.put(1) } }

    then:
    harness.yield { transfer.get() }.valueOrThrow.get() == 1
    harness.yield { transfer.tryGet(Duration.ofMillis(2)) }.valueOrThrow.empty
  }

  def "can cancel put"() {
    when:
    def result = new BlockingVariable<Boolean>()
    Thread.start { result.set(harness.yield { transfer.put(1) }.valueOrThrow) }
    Thread.start { harness.execute(transfer.close()) }

    then:
    result.get() == false
  }

  def "can cancel get"() {
    when:
    def result = new BlockingVariable<Optional<Integer>>()
    Thread.start { result.set(harness.yield { transfer.get() }.valueOrThrow) }
    Thread.start { harness.execute(transfer.close()) }

    then:
    result.get().empty
  }

}
