package net.robinfriedli.threadpool;

import java.util.concurrent.RejectedExecutionException;

public interface RejectedExecutionHandler {

    RejectedExecutionHandler ABORT_POLICY = (task, pool) -> {
        throw new RejectedExecutionException("Rejected execution of " + task + " from pool " + pool);
    };

    RejectedExecutionHandler DISCARD_POLICY = (task, pool) -> {
    };

    RejectedExecutionHandler DISCARD_OLDEST_POLICY = (task, pool) -> {
        if (!pool.isShutdown()) {
            pool.getWorkQueue().poll();
            pool.execute(task);
        }
    };

    RejectedExecutionHandler CALLER_RUNS_POLICY = (task, pool) -> {
        if (!pool.isShutdown()) {
            task.run();
        }
    };

    void handleRejectedExecution(Runnable task, ThreadPool pool);

}
