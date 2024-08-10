package reactive

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Supplier


object LearnReactor {
  @JvmStatic
  fun main(args: Array<String>) {
//    parallelPoolExample()

    elasticPoolExample()

//    flatMapElastic()

//    flatmapParallel()

//    flatMapBlocking()
    Thread.sleep(10000)
  }

  private fun parallelPoolExample() {
    Flux.range(1, 10)
      .parallel(2, 1)
      .runOn(Schedulers.newParallel("parallel", 10))
      .flatMap { i: Int? ->
        println(String.format("Start executing %s on thread %s", i, Thread.currentThread().name))
        doSomething()
        println(String.format("Finish executing %s", i))
        Mono.just(i)
      }.subscribe()
  }

  private fun elasticPoolExample() {
    Flux.range(1, 10)
      .parallel(10)
      .runOn(Schedulers.boundedElastic())
      .doOnNext { i: Int? ->
        println(String.format("${System.currentTimeMillis()} Executing %s on thread %s", i, Thread.currentThread().name))
        doSomething()
        println(String.format("${System.currentTimeMillis()} Finish executing %s", i))
      }
      .subscribe()
  }

  private fun flatMapElastic(){
    val scheduler: Scheduler = Schedulers.elastic()

    Flux.range(1, 10)
      .flatMap(
        { i: Int? ->
          Mono.defer {
            println(String.format("Executing %s on thread %s", i, Thread.currentThread().name))
            doSomething()
            println(String.format("Finish executing %s", i))
            Mono.just(i)
          }.subscribeOn(scheduler)
        }
        ,
        5
      )
      .subscribe { x: Int? ->  }
  }

  fun flatmapParallel(){
    val scheduler = Schedulers.newParallel("parallel", 2)

    Flux.range(1, 10)
      .flatMap(
        { i: Int? ->
          Mono.defer {
            println(String.format("Executing %s on thread %s", i, Thread.currentThread().name))
            Mono.delay(Duration.ofSeconds(i!!.toLong()))
              .flatMap { x: Long? ->
                println(String.format("Finish executing %s", i))
                Mono.just(i)
              }
          }.subscribeOn(scheduler)
        },
        5
      )
      .subscribe { x: Int? -> }
  }
  fun flatMapBlocking(){
    val scheduler = Schedulers.newParallel("parallel", 2)

    Flux.range(1, 10)
      .flatMap(
       { i: Int? ->
          Mono.defer{
            println(String.format("Executing %s on thread %s", i, Thread.currentThread().name))
            doSomething()
            println(String.format("Finish executing %s", i))
            Mono.just(1)
          }.subscribeOn(scheduler)
        },
        5
      )
      .log()
      .subscribe()
  }
  fun doSomething(){
    Thread.sleep(1000)
  }
}