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
 *
 */
public class ThreadPool extends AbstractExecutorService {

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
    private final WorkerCountData workerCountData = new WorkerCountData();

    private volatile boolean shutdown;
    private volatile boolean interrupt;

    public ThreadPool() {
        this(getNumCpus());
    }

    public ThreadPool(int coreSize) {
        this(coreSize, Math.max(coreSize * 4, Math.max(coreSize * 2, coreSize)), 60, TimeUnit.SECONDS);
    }

    public ThreadPool(int coreSize, int maxSize) {
        this(coreSize, maxSize, 60, TimeUnit.SECONDS);
    }

    public ThreadPool(int coreSize, int maxSize, long keepAliveTime, TimeUnit keepAliveUnit) {
        this(coreSize, maxSize, keepAliveTime, keepAliveUnit, new LinkedBlockingQueue<>());
    }

    public ThreadPool(int coreSize, int maxSize, long keepAliveTime, TimeUnit keepAliveUnit, BlockingQueue<Runnable> workQueue) {
        this(coreSize, maxSize, keepAliveTime, keepAliveUnit, workQueue, Executors.defaultThreadFactory());
    }

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

    @Override
    public void shutdown() {
        shutdown = true;
        // key set iterator is not guaranteed to see workers added after creation of the iterator, however the shutdown
        // flag has already been set so workers created after this point will execute their initial task (which is correct
        // since the task must have been submitted before the shutdown occurred) and then break their work loop upon
        // checking whether the pool has been shutdown after executing their task, provided the work queue is empty. If
        // the work queue is not empty each worker will help tidying up the remaining work, however after this point
        // workers will no longer block while polling from the queue but simply poll once and exit once they receive null.
        //
        // Furthermore workers transitioning from or into an idle state are not a concern since they check whether the
        // pool has been shut down on each transition (after receiving a task from the queue or after finishing a task);
        // the only goal here is to interrupt threads that are stuck on polling a task from the queue.
        for (Worker worker : workers.keySet()) {
            if (worker.idle) {
                worker.interrupt();
            }
        }
    }

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
        // key set iterator is not guaranteed to see workers added after creation of the iterator, however the interrupt
        // flag has already been set so workers created after this point will interrupt themselves either way
        for (Worker worker : workers.keySet()) {
            worker.interrupt();
        }
        return drain;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && workers.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException();
        }
        return join(timeout, unit);
    }

    public void awaitTermination() throws InterruptedException {
        join(null, null);
    }

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

    // getters

    public int getCoreSize() {
        return coreSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public TimeUnit getKeepAliveUnit() {
        return keepAliveUnit;
    }

    public BlockingQueue<Runnable> getWorkQueue() {
        return workQueue;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public RejectedExecutionHandler getRejectedExecutionHandler() {
        return rejectedExecutionHandler;
    }

    public int getCurrentWorkerCount() {
        return workerCountData.getTotalWorkerCount();
    }

    public int getIdleWorkerCount() {
        return workerCountData.getIdleWorkerCount();
    }

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

    private boolean join(Long timeout, TimeUnit unit) throws InterruptedException {
        WorkerCountPair workerCount = workerCountData.getBoth();
        int idleCount = workerCount.getIdleCount();
        int totalCount = workerCount.getTotalCount();
        if (idleCount == totalCount && workQueue.isEmpty()) {
            // no thread is currently doing any work
            return true;
        }

        joinLock.lock();
        try {
            if (timeout != null && unit != null) {
                return joinCondVar.await(timeout, unit);
            } else {
                joinCondVar.await();
                return true;
            }
        } finally {
            joinLock.unlock();
        }
    }

    private static int getNumCpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static class Builder {

        private Integer coreSize;
        private Integer maxSize;
        private Long keepAliveTime;
        private TimeUnit keepAliveUnit;
        private BlockingQueue<Runnable> workQueue;
        private ThreadFactory threadFactory;
        private RejectedExecutionHandler rejectedExecutionHandler;

        public static Builder create() {
            return new Builder();
        }

        public Integer getCoreSize() {
            return coreSize;
        }

        public Builder setCoreSize(Integer coreSize) {
            this.coreSize = coreSize;
            return this;
        }

        public Integer getMaxSize() {
            return maxSize;
        }

        public Builder setMaxSize(Integer maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Long getKeepAliveTime() {
            return keepAliveTime;
        }

        public TimeUnit getKeepAliveUnit() {
            return keepAliveUnit;
        }

        public Builder setKeepAlive(Long keepAliveTime, TimeUnit keepAliveUnit) {
            this.keepAliveTime = keepAliveTime;
            this.keepAliveUnit = keepAliveUnit;
            return this;
        }

        public BlockingQueue<Runnable> getWorkQueue() {
            return workQueue;
        }

        public Builder setWorkQueue(BlockingQueue<Runnable> workQueue) {
            this.workQueue = workQueue;
            return this;
        }

        public ThreadFactory getThreadFactory() {
            return threadFactory;
        }

        public Builder setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public RejectedExecutionHandler getRejectedExecutionHandler() {
            return rejectedExecutionHandler;
        }

        public Builder setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
            this.rejectedExecutionHandler = rejectedExecutionHandler;
            return this;
        }

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
                    if (oldWorkerCount.getTotalCount() == 1) {
                        ReentrantLock lock = pool.joinLock;
                        lock.lock();
                        try {
                            pool.joinCondVar.signalAll();
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
