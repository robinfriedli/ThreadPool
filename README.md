# ThreadPool

## Maven
```xml
    <dependency>
      <groupId>com.github.robinfriedli</groupId>
      <artifactId>ThreadPool</artifactId>
      <version>1.1.3</version>
      <type>pom</type>
    </dependency>

    <repository>
        <name>jitpack.io</name>
        <url>https://jitpack.io</url>
    </repository>
```

## Gradle
```gradle
    dependencies {
        implementation "com.github.robinfriedli:ThreadPool:1.1.3"
    }

    repositories {
        maven { url "https://jitpack.io" }
    }
```

## Requirements

Version 1.1 requires Java 9 due to the introduction of additional atomic operations (namely AtomicLong#compareAndExchange).
Versions below 1.1 require Java 8.

This repo is a Java port of the corresponding [Rust implementation](https://github.com/robinfriedli/rusty_pool)

Scalable self growing / shrinking ThreadPool implementation. This ThreadPool implements the ExecutorService
interface and offers an alternative for the JDK ThreadPoolExecutor implementation that prioritizes creating new worker
threads above queueing tasks for better potential throughput and flexibility.

This ThreadPool has two different pool sizes; a core pool size filled with threads that live for as long as the pool
and only exit once the pool is shut down, and a max pool size which describes the maximum number of worker threads
that may live at the same time. Those additional non-core threads have a specific keep-alive time specified when
creating the ThreadPool that defines how long such threads may be idle for without receiving any work before giving
up and terminating their work loop.

This ThreadPool does not spawn any threads until a task is submitted to it. Then it will create a new thread for each
task until the core pool is full. After that a new thread will only be created upon an #execute call
if the current pool size is lower than the max pool size and there are no idle threads. Additionally, a new worker is
always created if offering a task to the queue fails, and the pool size is below the maximum pool size.

This is one of the major differences in implementation compared to the ThreadPoolExecutor, which only creates a new worker
thread if the pool size is below the core size or if submitting the task to the queue fails. So when using an unbounded
work queue, that implementation never creates any additional threads above the core pool size since submitting a task to
the queue never fails. This means that the max pool size is only meaningful when using a bounded or zero-capacity queue
(e.g. SynchronousQueue). Both of these options have the disadvantage that the pool starts rejecting tasks if the max pool
size has been reached.

So, in essence, the ThreadPoolExecutor offers 3 options, all of which are suboptimal:

1. Using and unbounded queue; This option does not provide any flexibility as the pool will stay at the core pool size
unless core pool thread timeout is enabled, essentially meaning there is no core pool at all. The max pool size is
meaningless when choosing this option.

2. Using a bounded queue; In this case additional non-core workers are created after the bounded queue is full. If
the queue is full, and the pool is at its maximum size, the pool will start rejecting tasks.

3. Using a zero-capacity SynchronousQueue; With this option there technically is no queue.
Queue submissions always fail if there is no idle thread polling the queue. So new worker threads are spawned whenever
a task is submitted and there are no idle threads until the maximum size has been reached, which is when the pool
starts rejecting tasks.

Essentially the ThreadPoolExecutor forces users to choose between Executors#newFixedThreadPool
and not have a flexible pool or use Executors#newCachedThreadPool and deal with rejected executions if the
pool reaches its maximum size because there is no queue.

This ThreadPool allows users to use an unbounded queue to store up to Integer.MAX_VALUE tasks while having a
flexible pool that dynamically creates new worker threads if there are no idle threads.

Since this ThreadPool implements the ExecutorService interface, it offers the same methods to execute tasks.
The #execute method simply submits a task for execution to the ThreadPool. The #submit method returns a Future which
can be used to await the result of running the provided callable.

When creating a new worker this ThreadPool always re-checks whether the new worker is still required before spawning
a thread and passing it the submitted task in case an idle thread has opened up in the meantime or another thread has
already created the worker. If the re-check failed for a core worker the pool will try creating a new non-core worker
before deciding no new worker is needed. Worker threads that throw an exception while executing their task will get
revived immediately.

Locks are only used for the #join and #awaitTermination functionalities to obtain the monitor for
the respective Condition. Workers are stored in a ConcurrentHashMap and all bookkeeping is based on
atomic operations. This ThreadPool decides whether it is currently idle (and should fast-return join attempts) by
comparing the total worker count to the idle worker count, which are two 32-bit numbers stored in a single AtomicLong
making sure that if both should be updated they may be updated in a single atomic operation.

Usage:

Create a new ThreadPool:

```java
// create a ThreadPool with the default configuration based on the number of CPUs
ThreadPool pool = new ThreadPool();
// create a ThreadPool with a 5 core and 50 max pool size and a 60 second keep alive time for non-core workers
ThreadPool pool = new ThreadPool(5, 50, 60, TimeUnit.SECONDS);
// using the builder pattern
ThreadPool pool = ThreadPool.Builder.create().setCoreSize(5).setMaxSize(50).build();
```
<p>
Submit a task for execution by the ThreadPool:

```java
ThreadPool pool = new ThreadPool();
pool.execute(() -> {
    try {
        Thread.sleep(5000);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
    System.out.println("hello");
});
```
<p>
Submit a task and await the result:

```java
ThreadPool pool = new ThreadPool();
Future<Integer> future = pool.submit(() -> {
    try {
        Thread.sleep(5000);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
    return 4;
});
int result = future.get();
```
<p>
Join and shut down the ThreadPool:

```java
ThreadPool pool = new ThreadPool();
for (int i = 0; i < 10; i++) {
    pool.execute(() -> {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    });
}
// wait for all threads to become idle, i.e. all tasks to be completed including tasks added by other threads after
// join() is called by this thread, or for the timeout to be reached
pool.join(5, TimeUnit.SECONDS);

AtomicInteger counter = new AtomicInteger(0);
for (int i = 0; i < 15; i++) {
    pool.execute(() -> {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        counter.incrementAndGet();
    });
}

pool.shutdown();
pool.awaitTermination();
assertEquals(counter.get(), 15);
```
