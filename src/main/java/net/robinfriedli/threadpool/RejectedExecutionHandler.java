package net.robinfriedli.threadpool;

import java.util.concurrent.RejectedExecutionException;

/**
 * An implementation of this interface can be supplied when constructing a {@link ThreadPool} to define how rejected tasks
 * are handled. Tasks are rejected if the pool is busy (the pool size has reached the maximum pool size and not a single
 * thread is idle and waiting for a task) and the work queue rejected the tasks (e.g. if it is a bounded queue that is
 * already full). This interface includes a few default implementations.
 */
public interface RejectedExecutionHandler {

    /**
     * Throw a {@link RejectedExecutionException} if a task gets rejected. This is the default policy.
     */
    RejectedExecutionHandler ABORT_POLICY = (task, pool) -> {
        throw new RejectedExecutionException("Rejected execution of " + task + " from pool " + pool);
    };

    /**
     * Ignore rejected tasks. This implementation simply does nothing.
     */
    RejectedExecutionHandler DISCARD_POLICY = (task, pool) -> {
    };

    /**
     * Remove the task that otherwise would have been executed next (the task that is at the head of the queue and has
     * thus been in the queue for the longest) and try submitting the rejected task again.
     */
    RejectedExecutionHandler DISCARD_OLDEST_POLICY = (task, pool) -> {
        if (!pool.isShutdown()) {
            pool.getWorkQueue().poll();
            pool.execute(task);
        }
    };

    /**
     * Run the rejected task in the thread that tried to submit the task (the caller of the #execute method).
     */
    RejectedExecutionHandler CALLER_RUNS_POLICY = (task, pool) -> {
        if (!pool.isShutdown()) {
            task.run();
        }
    };

    void handleRejectedExecution(Runnable task, ThreadPool pool);

}
