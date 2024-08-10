package reactive

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

object MonoZip {
  @JvmStatic
  fun main(args: Array<String>) {
    Mono.zip(
      Mono.empty<Long>().doOnEach { println( "${Thread.currentThread().name} - ${it.context}") }.log().subscribeOn(Schedulers.boundedElastic()),
      Flux.fromIterable(listOf(1,2,3)).collectList().log().subscribeOn(Schedulers.boundedElastic())
    ).log().block()
    println("this code completed")
  }
}