package net.robinfriedli.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Scalable self growing / shrinking ThreadPool implementation. This ThreadPool implements the {@link java.util.concurrent.ExecutorService}
 * interface and offers an alternative for the {@link java.util.concurrent.ThreadPoolExecutor} implementation that
 * prioritizes creating new worker threads above queueing tasks for better potential throughput and flexibility.
 * <p>
 * This ThreadPool has two different pool sizes; a core pool size filled with threads that live for as long as the pool
 * and only exit once the pool is shut down and a max pool size which describes the maximum number of worker threads
 * that may live at the same time. Those additional non-core threads have a specific keep-alive time specified when
 * creating the ThreadPool that defines how long such threads may be idle for without receiving any work before giving
 * up and terminating their work loop.
 * <p>
 * This ThreadPool does not spawn any threads until a task is submitted to it. Then it will create a new thread for each
 * task until the core pool is full. After that a new thread will only be created upon an {@link #execute(Runnable)} call
 * if the current pool size is lower than the max pool size and there are no idle threads.
 * <p>
 * This is one of the major differences in implementation compared to the {@link java.util.concurrent.ThreadPoolExecutor},
 * which only creates a new worker thread if the pool size is below the core size or if submitting the task to the queue
 * fails. So when using an unbounded work queue, that implementation never creates any additional threads above the core
 * pool size since submitting a task to the queue never fails. This means that the max pool size is only meaningful when
 * using a bounded or zero-capacity queue {@link java.util.concurrent.SynchronousQueue}. Both of these options have the
 * disadvantage that the pool starts rejecting tasks if the max pool size has been reached.
 * <p>
 * So, in essence, {@link java.util.concurrent.ThreadPoolExecutor} offers 3 options, all of which are suboptimal:
 * <p>
 * 1: Using and unbounded queue; This option does not provide any flexibility as the pool will stay at the core pool size
 * unless core pool thread timeout is enabled, essentially meaning there is no core pool at all. The max pool size is
 * meaningless when choosing this option.
 * <p>
 * 2: Using a bounded queue; In this case additional non-core workers are created after the bounded queue is full. If
 * the queue is full and the pool is at its maximum size the pool will start rejecting tasks.
 * <p>
 * 3: Using a zero-capacity {@link java.util.concurrent.SynchronousQueue}; With this option there technically is no queue.
 * Queue submissions always fail if there is no idle thread polling the queue. So new worker threads are spawned whenever
 * a task is submitted and there are no idle threads until the maximum size has been reached, which is when the pool
 * starts rejecting tasks.
 * <p>
 * Essentially {@link java.util.concurrent.ThreadPoolExecutor} forces users to choose between {@link Executors#newFixedThreadPool(int)}
 * and not have a flexible pool or use {@link Executors#newCachedThreadPool()} and deal with rejected executions if the
 * pool reaches its maximum size because there is no queue.
 * <p>
 * This ThreadPool allows users to use an unbounded queue to store up to {@link Integer#MAX_VALUE} tasks while having a
 * flexible pool that dynamically creates new worker threads if there are no idle threads.
 * <p>
 * Since this ThreadPool implements the {@link java.util.concurrent.ExecutorService} interface it offers the same methods
 * to execute tasks. The {@link #execute(Runnable)} method simply submits a task for execution to the ThreadPool.
 * {@link #submit(java.util.concurrent.Callable)} can be used to await the result of running the provided callable.
 * <p>
 * When creating a new worker this ThreadPool always re-checks whether the new worker is still required before spawning
 * a thread and passing it the submitted task in case an idle thread has opened up in the meantime or another thread has
 * already created the worker. If the re-check failed for a core worker the pool will try creating a new non-core worker
 * before deciding no new worker is needed. Worker threads that throw an exception while executing their task will get
 * revived immediately.
 * <p>
 * Locks are only used for the {@link #join()} and {@link #awaitTermination()} functionalities to obtain the monitor for
 * the respective {@link Condition}. Workers are stored in a {@link ConcurrentHashMap} and all bookkeeping is based on
 * atomic operations. This ThreadPool decides whether it is currently idle (and should fast-return join attempts) by
 * comparing the total worker count to the idle worker count, which are two 32 bit numbers stored in a single {@link AtomicLong}
 * making sure that if both should be updated they may be updated in a single atomic operation.
 * <p>
 * Usage:
 * <p>
 * Create a new ThreadPool:
 *
 * <pre>{@code
 * // create a ThreadPool with the default configuration based on the number of CPUs
 * ThreadPool pool = new ThreadPool();
 * // create a ThreadPool with a 5 core and 50 max pool size and a 60 second keep alive time for non-core workers
 * ThreadPool pool = new ThreadPool(5, 50, 60, TimeUnit.SECONDS);
 * // using the builder pattern
 * ThreadPool pool = ThreadPool.Builder.create().setCoreSize(5).setMaxSize(50).build();
 * }</pre>
 * <p>
 * Submit a task for execution by the ThreadPool:
 *
 * <pre>{@code
 * ThreadPool pool = new ThreadPool();
 * pool.execute(() -> {
 *     try {
 *         Thread.sleep(5000);
 *     } catch (InterruptedException e) {
 *         throw new RuntimeException(e);
 *     }
 *     System.out.println("hello");
 * });
 * }</pre>
 * <p>
 * Submit a task and await the result:
 *
 * <pre>{@code
 * ThreadPool pool = new ThreadPool();
 * Future<Integer> future = pool.submit(() -> {
 *     try {
 *         Thread.sleep(5000);
 *     } catch (InterruptedException e) {
 *         throw new RuntimeException(e);
 *     }
 *     return 4;
 * });
 * int result = future.get();
 * }</pre>
 * <p>
 * Join and shut down the ThreadPool:
 * <pre>{@code
 * ThreadPool pool = new ThreadPool();
 * for (int i = 0; i < 10; i++) {
 *     pool.execute(() -> {
 *         try {
 *             Thread.sleep(10000);
 *         } catch (InterruptedException e) {
 *             throw new RuntimeException(e);
 *         }
 *     });
 * }
 * // wait for all threads to become idle, i.e. all tasks to be completed including tasks added by other threads after
 * // join() is called by this thread, or for the timeout to be reached
 * pool.join(5, TimeUnit.SECONDS);
 *
 * AtomicInteger counter = new AtomicInteger(0);
 * for (int i = 0; i < 15; i++) {
 *     pool.execute(() -> {
 *         try {
 *             Thread.sleep(10000);
 *         } catch (InterruptedException e) {
 *             throw new RuntimeException(e);
 *         }
 *
 *         counter.incrementAndGet();
 *     });
 * }
 *
 * pool.shutdown();
 * pool.awaitTermination();
 * assertEquals(counter.get(), 15);
 * }</pre>
 */
public class ThreadPool extends AbstractExecutorService {

    // dummy value for the workers key set
    private static final Object DUMMY = new Object();

    private final int coreSize;
    private final int maxSize;
    private final long keepAliveTime;
    private final TimeUnit keepAliveUnit;
    private final BlockingQueue<Runnable> workQueue;
    private final ThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectedExecutionHandler;

    private final ConcurrentHashMap<Worker, Object> workers = new ConcurrentHashMap<>();
    private final ReentrantLock joinLock = new ReentrantLock();
    private final Condition joinCondVar = joinLock.newCondition();
    private final Condition terminationCondVar = joinLock.newCondition();
    private final WorkerCountData workerCountData = new WorkerCountData();

    private volatile boolean shutdown;
    private volatile boolean interrupt;

    /**
     * Overloading constructor with the following default values:
     * <p>
     * coreSize = number of CPUs (from the perspective of the OS, counting each core / hyper thread)
     * <p>
     * maxSize = coreSize * 4 (or coreSize * 2 or equal to coreSize if operation causes overflow)
     * <p>
     * keepAliveTime = 60
     * <p>
     * keepAliveUnit = {@link TimeUnit#SECONDS}
     * <p>
     * workQueue = new unbounded {@link LinkedBlockingQueue}
     * <p>
     * threadFactory = {@link Executors#defaultThreadFactory()}
     * <p>
     * rejectedExecutionHandler = {@link RejectedExecutionHandler#ABORT_POLICY}
     */
    public ThreadPool() {
        this(getNumCpus());
    }

    /**
     * Overloading constructor with the following default values:
     * <p>
     * maxSize = coreSize * 4 (or coreSize * 2 or equal to coreSize if operation causes overflow)
     * <p>
     * keepAliveTime = 60
     * <p>
     * keepAliveUnit = {@link TimeUnit#SECONDS}
     * <p>
     * workQueue = new unbounded {@link LinkedBlockingQueue}
     * <p>
     * threadFactory = {@link Executors#defaultThreadFactory()}
     * <p>
     * rejectedExecutionHandler = {@link RejectedExecutionHandler#ABORT_POLICY}
     *
     * @param coreSize the number of core threads that stay alive for as long as this ThreadPool
     */
    public ThreadPool(int coreSize) {
        this(coreSize, Math.max(coreSize * 4, Math.max(coreSize * 2, coreSize)), 60, TimeUnit.SECONDS);
    }

    /**
     * Overloading constructor with the following default values:
     * <p>
     * keepAliveTime = 60
     * <p>
     * keepAliveUnit = {@link TimeUnit#SECONDS}
     * <p>
     * workQueue = new unbounded {@link LinkedBlockingQueue}
     * <p>
     * threadFactory = {@link Executors#defaultThreadFactory()}
     * <p>
     * rejectedExecutionHandler = {@link RejectedExecutionHandler#ABORT_POLICY}
     *
     * @param coreSize the number of core threads that stay alive for as long as this ThreadPool
     * @param maxSize  the maximum number of threads this pool can hold
     */
    public ThreadPool(int coreSize, int maxSize) {
        this(coreSize, maxSize, 60, TimeUnit.SECONDS);
    }

    /**
     * Overloading constructor with the following default values:
     * <p>
     * workQueue = new unbounded {@link LinkedBlockingQueue}
     * <p>
     * threadFactory = {@link Executors#defaultThreadFactory()}
     * <p>
     * rejectedExecutionHandler = {@link RejectedExecutionHandler#ABORT_POLICY}
     *
     * @param coreSize      the number of core threads that stay alive for as long as this ThreadPool
     * @param maxSize       the maximum number of threads this pool can hold
     * @param keepAliveTime the amount of time worker threads outside of the core pool stay alive while waiting for work
     * @param keepAliveUnit the time unit for the keepAliveTime
     */
    public ThreadPool(int coreSize, int maxSize, long keepAliveTime, TimeUnit keepAliveUnit) {
        this(coreSize, maxSize, keepAliveTime, keepAliveUnit, new LinkedBlockingQueue<>());
    }

    /**
     * Overloading constructor with the following default values:
     * <p>
     * threadFactory = {@link Executors#defaultThreadFactory()}
     * <p>
     * rejectedExecutionHandler = {@link RejectedExecutionHandler#ABORT_POLICY}
     *
     * @param coreSize      the number of core threads that stay alive for as long as this ThreadPool
     * @param maxSize       the maximum number of threads this pool can hold
     * @param keepAliveTime the amount of time worker threads outside of the core pool stay alive while waiting for work
     * @param keepAliveUnit the time unit for the keepAliveTime
     * @param workQueue     the queue used to queue tasks if the pool is busy, an unbounded {@link LinkedBlockingQueue} by default
     */
    public ThreadPool(int coreSize, int maxSize, long keepAliveTime, TimeUnit keepAliveUnit, BlockingQueue<Runnable> workQueue) {
        this(coreSize, maxSize, keepAliveTime, keepAliveUnit, workQueue, Executors.defaultThreadFactory());
    }

    /**
     * Overloading constructor with the following default values:
     * <p>
     * rejectedExecutionHandler = {@link RejectedExecutionHandler#ABORT_POLICY}
     *
     * @param coreSize      the number of core threads that stay alive for as long as this ThreadPool
     * @param maxSize       the maximum number of threads this pool can hold
     * @param keepAliveTime the amount of time worker threads outside of the core pool stay alive while waiting for work
     * @param keepAliveUnit the time unit for the keepAliveTime
     * @param workQueue     the queue used to queue tasks if the pool is busy, an unbounded {@link LinkedBlockingQueue} by default
     * @param threadFactory the factory used to create new worker threads
     */
    public ThreadPool(
        int coreSize,
        int maxSize,
        long keepAliveTime,
        TimeUnit keepAliveUnit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory
    ) {
        this(coreSize, maxSize, keepAliveTime, keepAliveUnit, workQueue, threadFactory, RejectedExecutionHandler.ABORT_POLICY);
    }

    /**
     * Construct a new ThreadPool. This action does not yet spawn any threads but initializes an empty ThreadPool.
     *
     * @param coreSize                 the number of core threads that stay alive for as long as this ThreadPool
     * @param maxSize                  the maximum number of threads this pool can hold
     * @param keepAliveTime            the amount of time worker threads outside of the core pool stay alive while waiting for work
     * @param keepAliveUnit            the time unit for the keepAliveTime
     * @param workQueue                the queue used to queue tasks if the pool is busy, an unbounded {@link LinkedBlockingQueue} by default
     * @param threadFactory            the factory used to create new worker threads
     * @param rejectedExecutionHandler the {@link RejectedExecutionHandler} used if the pool rejects tasks due to the queue
     *                                 being full and the pool being busy
     */
    public ThreadPool(
        int coreSize,
        int maxSize,
        long keepAliveTime,
        TimeUnit keepAliveUnit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        RejectedExecutionHandler rejectedExecutionHandler
    ) {
        this.coreSize = coreSize;
        this.maxSize = maxSize;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveUnit = keepAliveUnit;
        this.workQueue = workQueue;
        this.threadFactory = threadFactory;
        this.rejectedExecutionHandler = rejectedExecutionHandler;

        if (maxSize == 0 || maxSize < coreSize) {
            throw new IllegalArgumentException("Max size cannot be 0 or less than core size");
        }

        if (keepAliveUnit == null || workQueue == null || threadFactory == null || rejectedExecutionHandler == null) {
            throw new NullPointerException();
        }
    }

    // executor methods

    /**
     * Initiates a soft shutdown in which all currently running and all queued tasks will finish execution but no new
     * tasks will be accepted.
     * <p>
     * This interrupts all idle workers to inform them about the status change. After this point workers will no longer
     * block while waiting for tasks from the queue but simply poll the queue once and exit upon receiving <code>null</code>.
     * <p>
     * Threads that are currently waiting for termination (see {@link #awaitTermination()}) will be notified by this
     * method call if the pool is already empty. Else those threads are notified once the last worker exits.
     */
    @Override
    public void shutdown() {
        shutdown = true;
        handleWorkerTermination(true);
    }

    /**
     * Initiates a hard shutdown in which all currently running tasks are interrupted (i.e. receive an interrupt signal,
     * causing the task to throw an InterruptedException when performing a blocking operation, if the task does not
     * check the interrupt flag it is unaffected) and all queued tasks are discarded.
     * <p>
     * This interrupts all workers and causes them to break their work loop without executing any further tasks. In case
     * a race occurs where this method is called just after a worker is created but before it can start its initial task
     * (or after a worker receives a task from the queue but before starting that task, though less likely due to lower
     * time delta); the worker thread interrupts itself before starting its current task.
     *
     * @return all tasks that were in the queue when this method was called and never had the chance to run
     */
    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        interrupt = true;
        List<Runnable> drain = new ArrayList<>(workQueue.size());
        workQueue.drainTo(drain);
        if (!workQueue.isEmpty()) {
            for (Runnable r : workQueue.toArray(new Runnable[0])) {
                if (workQueue.remove(r)) {
                    drain.add(r);
                }
            }
        }
        handleWorkerTermination(false);
        return drain;
    }

    /**
     * @return true if this ThreadPool has been or is in the process of shutting down (i.e. if {@link #shutdown()} or
     * {@link #shutdownNow()} has been called)
     */
    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * @return true if this ThreadPool has been shutdown and all its workers have died (i.e. if {@link #shutdown()} or
     * {@link #shutdownNow()} has been called and all workers have exited)
     */
    @Override
    public boolean isTerminated() {
        return shutdown && workers.isEmpty();
    }

    /**
     * Await the termination of this ThreadPool. This blocks the current thread until {@link #shutdown()} or {@link #shutdownNow()}
     * has been called and all workers have died. After calling this method {@link #isTerminated()} is guaranteed to return
     * <code>true</code>. Additionally, this method returns once the provided timeout has been reached, returning
     * <code>false</code>. This is always the case if this pool is not already shutting down and no other thread shuts down
     * this ThreadPool during this time.
     *
     * @param timeout the maximum amount of time to wait for the termination of the ThreadPool
     * @param unit    the time unit of timeout
     * @return <code>true</code> if the pool has been shut down or <code>false</code> if the timeout has been reached
     * before that happened
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException();
        }
        return doAwaitTermination(timeout, unit);
    }

    /**
     * Await the termination of this ThreadPool. This blocks the current thread until {@link #shutdown()} or {@link #shutdownNow()}
     * has been called and all workers have died. After calling this method {@link #isTerminated()} is guaranteed to return
     * <code>true</code>. Note that if the pool is not already shutting down and no other thread ever signals the
     * ThreadPool to shut down this thread will be stuck waiting for ever.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitTermination() throws InterruptedException {
        doAwaitTermination(null, null);
    }

    /**
     * Join all tasks in this ThreadPool. This blocks the current thread until all tasks in this ThreadPool have been
     * completed, including tasks submitted by other threads after this method has been called. Essentially this waits
     * for the next time the pool to become idle, meaning all worker threads are idle and the queue is empty. Additionally,
     * this method returns once the provided timeout has been reached, returning <code>false</code>.
     * <p>
     * This thread is notified once a worker completes a task and notices that it is the last thread to become idle and
     * there is no work in the queue. If the pool is idle when calling this method, meaning all workers are idle
     * and the queue is empty, the method will return immediately without blocking.
     * <p>
     * If a race occurs where the last worker to complete a task does not notify joiners because there is still work in
     * the queue but none of that work is ever executed because {@link #shutdownNow()} is called immediately after; this
     * thread will get notified once the last worker dies along with threads waiting for {@link #awaitTermination()}.
     * This race condition is quite rare in pools with more than one thread because there has to be a point in time
     * where all threads are idle when the last worker becomes idle while there is still work in the queue. Even with 1
     * thread the time delta between the thread executing its previous task and starting to execute the next one is
     * small enough for this to rarely occur in practice.
     *
     * @param timeout the maximum amount of time to wait for the pool to become idle
     * @param unit    the time unit of timeout
     * @return <code>true</code> if the join was successful, <code>false</code> if the timeout was reached
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean join(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException();
        }
        return doJoin(timeout, unit);
    }

    /**
     * Join all tasks in this ThreadPool. This blocks the current thread until all tasks in this ThreadPool have been
     * completed, including tasks submitted by other threads after this method has been called. Essentially this waits
     * for the next time the pool to become idle, meaning all worker threads are idle and the queue is empty.
     * <p>
     * This thread is notified once a worker completes a task and notices that it is the last thread to become idle and
     * there is no work in the queue. If the pool is idle when calling this method, meaning all workers are idle
     * and the queue is empty, the method will return immediately without blocking.
     * <p>
     * If a race occurs where the last worker to complete a task does not notify joiners because there is still work in
     * the queue but none of that work is ever executed because {@link #shutdownNow()} is called immediately after; this
     * thread will get notified once the last worker dies along with threads waiting for {@link #awaitTermination()}.
     * This race condition is quite rare in pools with more than one thread because there has to be a point in time
     * where all threads are idle when the last worker becomes idle while there is still work in the queue. Even with 1
     * thread the time delta between the thread executing its previous task and starting to execute the next one is
     * small enough for this to rarely occur in practice.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void join() throws InterruptedException {
        doJoin(null, null);
    }

    /**
     * Submit a task for execution. If called after the pool has been shutdown by {@link #shutdown()} or {@link #shutdownNow()}
     * the {@link RejectedExecutionHandler} specified when initializing the pool is called immediately. This method might
     * spawn a new worker thread if the current pool size is lower than the core pool size or the current pool size is lower
     * than the maximum pool size and there are no idle threads. Else the submitted task is offered to the workQueue, if
     * that fails because the queue rejects the task (e.g. when using a bounded queue and the queue is full) the
     * {@link RejectedExecutionHandler} is called.
     *
     * @param command the runnable to run
     */
    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException();
        }
        if (shutdown) {
            rejectedExecutionHandler.handleRejectedExecution(command, this);
            return;
        }

        WorkerCountPair currentWorkerCountData = workerCountData.getBoth();
        int totalCount = currentWorkerCountData.getTotalCount();
        int idleCount = currentWorkerCountData.getIdleCount();

        if (totalCount < coreSize) {
            if (!createWorker(true, command)) {
                if (!workQueue.offer(command)) {
                    rejectedExecutionHandler.handleRejectedExecution(command, this);
                }
            }
        } else if (totalCount < maxSize && idleCount == 0) {
            if (!createWorker(false, command)) {
                if (!workQueue.offer(command)) {
                    rejectedExecutionHandler.handleRejectedExecution(command, this);
                }
            }
        } else {
            if (!workQueue.offer(command)) {
                rejectedExecutionHandler.handleRejectedExecution(command, this);
            }
        }
    }

    /**
     * @return a human readable string describing this ThreadPool including the following stats:
     * <p>
     * shut down: whether or not the pool has been (or is in the process of) shut down by {@link #shutdown()} or {@link #shutdownNow()}
     * <p>
     * interrupted: whether or not the pool has been interrupted (i.e. whether or not {@link #shutdownNow()} has been called)
     * <p>
     * terminated: whether or not the pool has been shut down and all workers have died (see {@link #isTerminated()})
     * <p>
     * idle workers: number of worker threads that are idle and waiting for work
     * <p>
     * total workers: total number of worker threads in this pool
     * <p>
     * queue size: number of tasks in the queue
     */
    @Override
    public String toString() {
        WorkerCountPair workerData = workerCountData.getBoth();
        return String.format("%s[shut down: %s, interrupted: %s, terminated: %s, idle workers: %d, total workers: %d, queue size: %d]",
            super.toString(),
            shutdown,
            interrupt,
            isTerminated(),
            workerData.getIdleCount(),
            workerData.getTotalCount(),
            workQueue.size());
    }

    // getters

    /**
     * @return the core pool size limit (i.e. number of threads that stay alive for as long as the pool)
     */
    public int getCoreSize() {
        return coreSize;
    }

    /**
     * @return the maximum pool size of this ThreadPool
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * @return the amount of time non-core workers stay alive while idly waiting for work
     */
    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    /**
     * @return the time unit for {@link #getKeepAliveTime()}
     */
    public TimeUnit getKeepAliveUnit() {
        return keepAliveUnit;
    }

    /**
     * @return the work queue used to queue tasks when the pool is busy
     */
    public BlockingQueue<Runnable> getWorkQueue() {
        return workQueue;
    }

    /**
     * @return the {@link ThreadFactory} used to create new worker threads
     */
    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    /**
     * @return the {@link RejectedExecutionHandler} used when the pool rejects tasks when the queue is full
     */
    public RejectedExecutionHandler getRejectedExecutionHandler() {
        return rejectedExecutionHandler;
    }

    /**
     * @return the amount of worker threads currently alive in this pool, whether idle or not
     */
    public int getCurrentWorkerCount() {
        return workerCountData.getTotalWorkerCount();
    }

    /**
     * @return the amount of worker threads in this pool that are currently idle and waiting for work
     */
    public int getIdleWorkerCount() {
        return workerCountData.getIdleWorkerCount();
    }

    private static boolean awaitCondition(ReentrantLock lock, Condition condition, Long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            if (timeout != null && unit != null) {
                return condition.await(timeout, unit);
            } else {
                condition.await();
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    private static int getNumCpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Create a new worker thread. This atomically increments the total worker count of the worker count data and uses
     * the previous value to determine whether the new worker is still needed. If trying to create a core worker and noticing
     * the core pool is already full (i.e. the previous worker total is already equal to the core size limit) this method
     * attempts to create a non-core worker instead. If the max pool size has already been reached (i.e. the previous total
     * is already equal to the max pool size limit) this method returns false, causing the task to be submitted to the
     * queue instead.
     *
     * @param core whether or not the create worker should be a core thread
     * @param task the initial task for the new worker to execute
     * @return whether or not a new worker has actually been created
     */
    private boolean createWorker(boolean core, Runnable task) {
        WorkerCountPair prevWorkerCountData = workerCountData.incrementWorkerTotalRetBoth();
        // recheck that the worker is still required or if another thread has already created one
        if ((core && prevWorkerCountData.getTotalCount() < coreSize)
            || (!core && prevWorkerCountData.getTotalCount() < maxSize && prevWorkerCountData.getIdleCount() == 0)) {
            new Worker(!core, task).start();
        } else {
            workerCountData.decrementWorkerTotal();

            if (core && prevWorkerCountData.getTotalCount() < maxSize && prevWorkerCountData.getIdleCount() == 0) {
                // try create a non-core worker instead if the core pool has been filled up while trying to create this worker
                return createWorker(false, task);
            }

            return false;
        }

        return true;
    }

    private boolean doAwaitTermination(Long timeout, TimeUnit unit) throws InterruptedException {
        if (isTerminated()) {
            return true;
        }

        return awaitCondition(joinLock, terminationCondVar, timeout, unit);
    }

    private boolean doJoin(Long timeout, TimeUnit unit) throws InterruptedException {
        WorkerCountPair workerCount = workerCountData.getBoth();
        int idleCount = workerCount.getIdleCount();
        int totalCount = workerCount.getTotalCount();
        if (idleCount == totalCount && workQueue.isEmpty()) {
            // no thread is currently doing any work
            return true;
        }

        return awaitCondition(joinLock, joinCondVar, timeout, unit);
    }

    private void handleWorkerTermination(boolean idleOnly) {
        // notify joiners waiting for termination if the pool is already empty
        if (workers.isEmpty()) {
            joinLock.lock();
            try {
                terminationCondVar.signalAll();
            } finally {
                joinLock.unlock();
            }
        } else {
            // key set iterator is not guaranteed to see workers added after creation of the iterator, however the shutdown
            // flag has already been set so workers created after this point will execute their initial task (which is correct
            // since the task must have been submitted before the shutdown occurred) and then break their work loop upon
            // checking whether the pool has been shutdown after executing their task, provided the work queue is empty. If
            // the work queue is not empty each worker will help tidying up the remaining work, however after this point
            // workers will no longer block while polling from the queue but simply poll once and exit once they receive null.
            //
            // if shutdownNow() was used that means the pool was interrupted and workers added after this point interrupt
            // themselves
            //
            // Furthermore workers transitioning from or into an idle state are not a concern since they check whether the
            // pool has been shut down on each transition (after receiving a task from the queue or after finishing a task);
            // the only goal here is to interrupt threads that are stuck on polling a task from the queue.
            for (Worker worker : workers.keySet()) {
                if (!idleOnly || worker.idle) {
                    worker.interrupt();
                }
            }
        }
    }

    /**
     * Builder for the ThreadPool to easily specify a custom configuration while leaving undefined values to the default
     * configuration.
     */
    public static class Builder {

        private Integer coreSize;
        private Integer maxSize;
        private Long keepAliveTime;
        private TimeUnit keepAliveUnit;
        private BlockingQueue<Runnable> workQueue;
        private ThreadFactory threadFactory;
        private RejectedExecutionHandler rejectedExecutionHandler;

        /**
         * @return a new Builder
         */
        public static Builder create() {
            return new Builder();
        }

        /**
         * @return the core size specified for this builder
         */
        public Integer getCoreSize() {
            return coreSize;
        }

        /**
         * Define the core pool size.
         *
         * @param coreSize the core pool size
         * @return this builder for method chaining
         */
        public Builder setCoreSize(Integer coreSize) {
            this.coreSize = coreSize;
            return this;
        }

        /**
         * @return the specified maximum pool size
         */
        public Integer getMaxSize() {
            return maxSize;
        }

        /**
         * Define the maximum pool size.
         *
         * @param maxSize the max pool size
         * @return this builder for method chaining
         */
        public Builder setMaxSize(Integer maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        /**
         * @return the specified keepAliveTime
         */
        public Long getKeepAliveTime() {
            return keepAliveTime;
        }

        /**
         * @return the time unit used for {@link #getKeepAliveTime()}
         */
        public TimeUnit getKeepAliveUnit() {
            return keepAliveUnit;
        }

        /**
         * Define the time and time unit to keep non-core worker threads alive when idle.
         *
         * @param keepAliveTime the time to keep non-core workers alive when idle
         * @param keepAliveUnit the time unit for keepAliveTime
         * @return this builder for method chaining
         */
        public Builder setKeepAlive(Long keepAliveTime, TimeUnit keepAliveUnit) {
            this.keepAliveTime = keepAliveTime;
            this.keepAliveUnit = keepAliveUnit;
            return this;
        }

        /**
         * @return the specified workQueue
         */
        public BlockingQueue<Runnable> getWorkQueue() {
            return workQueue;
        }

        /**
         * Define the workQueue used to queue tasks when the pool is busy.
         *
         * @param workQueue the workQueue
         * @return this builder for method chaining
         */
        public Builder setWorkQueue(BlockingQueue<Runnable> workQueue) {
            this.workQueue = workQueue;
            return this;
        }

        /**
         * @return the specified {@link ThreadFactory}
         */
        public ThreadFactory getThreadFactory() {
            return threadFactory;
        }

        /**
         * Define the {@link ThreadFactory} used to create new worker threads.
         *
         * @param threadFactory the threadFactory
         * @return this builder for method chaining
         */
        public Builder setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * @return the specified {@link RejectedExecutionHandler}
         */
        public RejectedExecutionHandler getRejectedExecutionHandler() {
            return rejectedExecutionHandler;
        }

        /**
         * Define the {@link RejectedExecutionHandler} called by the pool when submitting a task while the pool is busy
         * and the queue is full.
         *
         * @param rejectedExecutionHandler the {@link RejectedExecutionHandler}
         * @return this builder for method chaining
         */
        public Builder setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
            this.rejectedExecutionHandler = rejectedExecutionHandler;
            return this;
        }

        /**
         * Use the information previously given to this builder to build a ThreadPool instance with the following default
         * values:
         * <p>
         * coreSize = number of CPUs (from the perspective of the OS, counting each core / hyper thread)
         * <p>
         * maxSize = coreSize * 4 (or coreSize * 2 or equal to coreSize if operation causes overflow)
         * <p>
         * keepAliveTime = 60
         * <p>
         * keepAliveUnit = {@link TimeUnit#SECONDS}
         * <p>
         * workQueue = new unbounded {@link LinkedBlockingQueue}
         * <p>
         * threadFactory = {@link Executors#defaultThreadFactory()}
         * <p>
         * rejectedExecutionHandler = {@link RejectedExecutionHandler#ABORT_POLICY}
         * <p>
         * If the coreSize is not defined but the maxSize is this makes sure that the coreSize is lower or equal to the
         * maxSize.
         *
         * @return the created ThreadPool
         */
        public ThreadPool build() {
            int coreSize = reqNonNullElse(this.coreSize, () -> {
                int numCpus = getNumCpus();
                if (this.maxSize != null) {
                    if (numCpus <= this.maxSize) {
                        return numCpus;
                    } else {
                        return this.maxSize;
                    }
                } else {
                    return numCpus;
                }
            });
            // handle potential integer overflow: try using twice or four times the core_size or return the
            // first result that did not overflow
            int maxSize = reqNonNullElse(this.maxSize, () -> Math.max(coreSize * 4, Math.max(coreSize * 2, coreSize)));
            long keepAliveTime = reqNonNullElse(this.keepAliveTime, 60L);
            TimeUnit keepAliveUnit = reqNonNullElse(this.keepAliveUnit, TimeUnit.SECONDS);
            BlockingQueue<Runnable> workQueue = reqNonNullElse(this.workQueue, LinkedBlockingQueue::new);
            ThreadFactory threadFactory = reqNonNullElse(this.threadFactory, Executors::defaultThreadFactory);
            RejectedExecutionHandler rejectedExecutionHandler = reqNonNullElse(this.rejectedExecutionHandler, RejectedExecutionHandler.ABORT_POLICY);

            return new ThreadPool(
                coreSize,
                maxSize,
                keepAliveTime,
                keepAliveUnit,
                workQueue,
                threadFactory,
                rejectedExecutionHandler
            );
        }

        private static <T> T reqNonNullElse(T obj, Supplier<T> alt) {
            if (obj == null) {
                return alt.get();
            }

            return obj;
        }

        private static <T> T reqNonNullElse(T obj, T alt) {
            if (obj == null) {
                return alt;
            }

            return obj;
        }
    }

    private final class Worker implements Runnable {

        private final boolean canTimeOut;
        private final Runnable initialTask;
        private final Thread thread;

        private volatile boolean idle;

        Worker(boolean canTimeOut, Runnable initialTask) {
            this.canTimeOut = canTimeOut;
            this.initialTask = initialTask;
            thread = getThreadFactory().newThread(this);
            idle = initialTask == null;
        }

        @Override
        public void run() {
            boolean revived = false;
            ThreadPool pool = ThreadPool.this;
            try {
                boolean continueLoop = true;
                if (initialTask != null) {
                    continueLoop = execTaskAndNotify(initialTask);
                }

                while (continueLoop) {
                    Runnable task;
                    try {
                        if (pool.shutdown) {
                            task = pool.workQueue.poll();
                        } else if (canTimeOut) {
                            task = pool.workQueue.poll(pool.keepAliveTime, pool.keepAliveUnit);
                        } else {
                            task = pool.workQueue.take();
                        }
                    } catch (InterruptedException e) {
                        if (pool.interrupt || (pool.shutdown && pool.workQueue.isEmpty())) {
                            // worker loop interrupt
                            break;
                        } else {
                            continue;
                        }
                    }

                    if (task == null || pool.interrupt) {
                        break;
                    }
                    idle = false;
                    pool.workerCountData.decrementWorkerIdle();
                    continueLoop = execTaskAndNotify(task);
                }
            } catch (Exception e) {
                // revive worker
                new Worker(canTimeOut, null).start();
                // new worker is always marked idle since initialTask == null
                // this is guaranteed to be reflected by the worker count data since the worker is only ever non-idle
                // when running execTaskAndNotify(), which always increments the idle count when done
                revived = true;
                throw e;
            } finally {
                pool.workers.remove(this);
                if (!revived) {
                    // worker only gets here from an idle state -> always decrement both counters
                    // workers are only ever non-idle during (and immediately before) a call to #execTaskAndNotify
                    // (if the worker was newly created and executing its initial task or after polling a task from the
                    // queue) and this method always returns them back to an idle state
                    WorkerCountPair oldWorkerCount = pool.workerCountData.decrementBoth();
                    if (oldWorkerCount.getTotalCount() == 1 && pool.shutdown) {
                        ReentrantLock lock = pool.joinLock;
                        lock.lock();
                        try {
                            // notify joiners once the last worker exits in case a shutdown happened at an inconvenient point
                            // in time where the last worker to finish any work and become idle sees that there is still work
                            // in the queue and thus does not notify joiners but none of that work is ever executed because the
                            // pool was shut down and interrupted (#shutdownNow) immediately after.
                            //
                            // For this problem to occur the following race condition needs to happen, which is most likely for
                            // a pool with a single thread:
                            // A worker thread finishes its task and increments and compares the worker counts and checks whether
                            // the queue is empty to decide if joiners should get notified. Finding that the queue is not empty
                            // the worker decides against notifying and continues the work loop. However after polling the next
                            // task from the queue but before beginning its execution (hence it is most likely for single threaded
                            // pools) the worker notices that the pool has been interrupted via #shutdownNow, thus the task is
                            // never executed. To provoke this race condition for testing the period between finishing execution
                            // of the last task and polling the queue generally has to be extended artificially through, e.g by
                            // adding a sleep statement (see test case #testShutDownNowImmediatelyAfterTaskExecutionWithQueuedTasks).
                            //
                            // in the worst case joiners that observed the pool to be busy but were never notified after the last
                            // task was executed due to the aforementioned situation will be notified now that the last worker
                            // shut down
                            pool.joinCondVar.signalAll();
                            pool.terminationCondVar.signalAll();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
        }

        void start() {
            ThreadPool.this.workers.put(this, DUMMY);
            thread.start();
        }

        void interrupt() {
            thread.interrupt();
        }

        private boolean execTaskAndNotify(Runnable task) {
            ThreadPool pool = ThreadPool.this;
            try {
                if ((pool.interrupt || (Thread.interrupted() && pool.interrupt))
                    && !thread.isInterrupted()) {
                    thread.interrupt();
                }
                task.run();
                // check also serves to clear interrupt flag
                return !(Thread.interrupted() && pool.interrupt || pool.interrupt);
            } finally {
                WorkerCountPair workerCountPair = pool.workerCountData.incrementWorkerIdleRetBoth();
                int previousTotal = workerCountPair.getTotalCount();
                int previousIdle = workerCountPair.getIdleCount();
                if (pool.workQueue.isEmpty() && previousTotal == previousIdle + 1) {
                    ReentrantLock lock = pool.joinLock;
                    lock.lock();
                    try {
                        pool.joinCondVar.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
                idle = true;
            }
        }

    }

    /**
     * Helper class that manages an {@link AtomicLong} that holds two 32 bit numbers. The upper 32 bits store the total
     * worker count and the lower 32 bits store the idle worker count.
     */
    static final class WorkerCountData {

        private static final long WORKER_IDLE_MASK = 0x0000_0000_FFFF_FFFF;

        private final AtomicLong workerCountData = new AtomicLong(0);

        int getTotalWorkerCount() {
            return getTotalCount(workerCountData.get());
        }

        int getIdleWorkerCount() {
            return getIdleCount(workerCountData.get());
        }

        WorkerCountPair getBoth() {
            return split(workerCountData.get());
        }

        WorkerCountPair incrementBoth() {
            long prev = workerCountData.getAndAdd(0x0000_0001_0000_0001L);
            return split(prev);
        }

        WorkerCountPair decrementBoth() {
            long negativeVal = (~0x0000_0001_0000_0001L) + 1;
            return split(workerCountData.getAndAdd(negativeVal));
        }

        int incrementWorkerTotal() {
            return getTotalCount(workerCountData.getAndAdd(0x0000_0001_0000_0000L));
        }

        WorkerCountPair incrementWorkerTotalRetBoth() {
            return split(workerCountData.getAndAdd(0x0000_0001_0000_0000L));
        }

        int decrementWorkerTotal() {
            long negativeVal = (~0x0000_0001_0000_0000L) + 1;
            return getTotalCount(workerCountData.getAndAdd(negativeVal));
        }

        WorkerCountPair decrementWorkerTotalRetBoth() {
            long negativeVal = (~0x0000_0001_0000_0000L) + 1;
            return split(workerCountData.getAndAdd(negativeVal));
        }

        int incrementWorkerIdle() {
            return getIdleCount(workerCountData.getAndAdd(0x0000_0000_0000_0001L));
        }

        WorkerCountPair incrementWorkerIdleRetBoth() {
            return split(workerCountData.getAndAdd(0x0000_0000_0000_0001L));
        }

        int decrementWorkerIdle() {
            long negativeVal = (~0x0000_0000_0000_0001L) + 1;
            return getIdleCount(workerCountData.getAndAdd(negativeVal));
        }

        WorkerCountPair decrementWorkerIdleRetBoth() {
            long negativeVal = (~0x0000_0000_0000_0001L) + 1;
            return split(workerCountData.getAndAdd(negativeVal));
        }

        static WorkerCountPair split(long val) {
            int totalCount = (int) (val >> 32);
            int idleCount = (int) (val & WORKER_IDLE_MASK);
            return new WorkerCountPair(totalCount, idleCount);
        }

        static int getIdleCount(long val) {
            return (int) (val & WORKER_IDLE_MASK);
        }

        static int getTotalCount(long val) {
            return (int) (val >> 32);
        }

    }

    static final class WorkerCountPair {

        private final int totalCount;
        private final int idleCount;

        WorkerCountPair(int totalCount, int idleCount) {
            this.totalCount = totalCount;
            this.idleCount = idleCount;
        }

        public int getTotalCount() {
            return totalCount;
        }

        public int getIdleCount() {
            return idleCount;
        }
    }

}
