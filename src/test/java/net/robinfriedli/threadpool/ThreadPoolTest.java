package net.robinfriedli.threadpool;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.*;

import static com.google.common.truth.Truth.*;
import static org.testng.Assert.*;

public class ThreadPoolTest {

    @Test
    public void itWorks() throws InterruptedException {
        ThreadPool pool = new ThreadPool(2, 10, 2, TimeUnit.SECONDS);
        AtomicInteger count = new AtomicInteger(0);

        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        pool.execute(() -> {
            count.incrementAndGet();
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(20000);

        int countResult = count.get();
        int currentWorkerCount = pool.getCurrentWorkerCount();

        assertEquals(countResult, 8);
        assertEquals(currentWorkerCount, 2);
        assertEquals(pool.getIdleWorkerCount(), 2);
    }

    @Test
    public void testJoin() throws InterruptedException {
        ThreadPool pool = new ThreadPool(0, 1, 5, TimeUnit.SECONDS);
        AtomicInteger count = new AtomicInteger(0);

        pool.execute(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            count.incrementAndGet();
        });
        pool.execute(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            count.incrementAndGet();
        });

        pool.join();
        assertEquals(count.get(), 2);
    }

    @Test
    public void testJoinTimeout() throws InterruptedException {
        ThreadPool pool = new ThreadPool(0, 1, 5, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        pool.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            counter.incrementAndGet();
        });

        pool.join(5, TimeUnit.SECONDS);
        assertEquals(counter.get(), 0);
    }

    @Test
    public void testShutdown() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1, 3, 3, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        // since the pool only allows three threads the 4th task won't get the chance to run
        for (int i = 0; i < 4; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                counter.incrementAndGet();
            });
        }

        pool.join(2, TimeUnit.SECONDS);
        pool.shutdown();

        Thread.sleep(5000);
        assertEquals(counter.get(), 3);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void test0MaxPoolSize() {
        new ThreadPool(0, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSmallerMaxThanCorePoolSize() {
        new ThreadPool(10, 4);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeMaxPoolSize() {
        new ThreadPool(-5, -3);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeCorePoolSize() {
        new ThreadPool(-1, 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeKeepAliveTime() {
        new ThreadPool(1, 1, -1, TimeUnit.SECONDS);
    }

    @Test
    public void testEmptyJoin() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 10, 10, TimeUnit.SECONDS);
        // test that the join doesn't last forever but fast-returns if the pool is idle
        pool.join();
    }

    @Test
    public void testJoinWhenComplete() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 10, 5, TimeUnit.SECONDS);

        pool.execute(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(5000);
        pool.join();
    }

    @Test
    public void testFullUsage() throws InterruptedException {
        ThreadPool pool = new ThreadPool(5, 50, 10, TimeUnit.SECONDS);

        for (int i = 0; i < 100; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        Thread.sleep(10000);
        assertEquals(pool.getCurrentWorkerCount(), 50);

        pool.join();
        Thread.sleep(15000);
        assertEquals(pool.getCurrentWorkerCount(), 5);
    }

    @Test
    public void testShutdownJoin() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1, 1, 5, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        pool.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            counter.incrementAndGet();
        });

        // give the task the chance to run, without sleeping the thread pool is shut down before the worker thread is
        // spawned and starts its first task
        Thread.sleep(1000);
        pool.shutdown();
        pool.join();
        assertEquals(counter.get(), 1);
    }

    @Test
    public void testShutdownJoinTimeout() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1, 1, 5, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        pool.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            counter.incrementAndGet();
        });

        pool.shutdown();
        pool.join(5, TimeUnit.SECONDS);
        assertEquals(counter.get(), 0);
        pool.join();
        assertEquals(counter.get(), 1);
    }

    @Test
    public void testEmptyShutdownJoin() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1, 5, 5, TimeUnit.SECONDS);
        pool.shutdown();
        pool.join();
        assertEquals(pool.getCurrentWorkerCount(), 0);
        assertTrue(pool.isTerminated());
    }

    @Test
    public void testEmptyShutdownAwait() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1, 5, 5, TimeUnit.SECONDS);
        pool.shutdown();
        pool.awaitTermination();
        assertEquals(pool.getCurrentWorkerCount(), 0);
        assertTrue(pool.isTerminated());
    }

    @Test
    public void testShutdownCorePool() throws InterruptedException {
        ThreadPool pool = new ThreadPool(5, 5);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 7; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                counter.incrementAndGet();
            });
        }

        assertEquals(pool.getCurrentWorkerCount(), 5);
        assertEquals(pool.getIdleWorkerCount(), 0);
        pool.shutdown();
        pool.awaitTermination();
        assertEquals(counter.get(), 7);

        // give the workers time to exit
        Thread.sleep(50);
        assertEquals(pool.getCurrentWorkerCount(), 0);
        assertEquals(pool.getIdleWorkerCount(), 0);
        assertTrue(pool.isTerminated());
    }

    @Test
    public void testShutdownIdleCorePool() throws InterruptedException {
        ThreadPool pool = new ThreadPool(5, 5);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            pool.execute(counter::incrementAndGet);
        }

        pool.join();
        assertEquals(counter.get(), 5);
        pool.shutdown();
        Thread.sleep(50);
        assertEquals(pool.getCurrentWorkerCount(), 0);
        assertEquals(pool.getIdleWorkerCount(), 0);
    }

    @Test
    public void testShutdownOnComplete() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 10, 5, TimeUnit.SECONDS);

        pool.execute(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(5000);
        pool.shutdown();
        pool.awaitTermination();
    }

    @Test
    public void testShutdownAfterComplete() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 10, 5, TimeUnit.SECONDS);

        pool.execute(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(7000);
        pool.shutdown();
        pool.awaitTermination();
    }

    @Test
    public void testIsTerminated() throws InterruptedException {
        ThreadPool pool = new ThreadPool();
        AtomicBoolean interruptFlag = new AtomicBoolean(false);

        pool.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                try {
                    Thread.sleep(6000);
                    interruptFlag.set(true);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        });

        pool.shutdownNow();
        pool.awaitTermination();
        assertTrue(pool.isTerminated());
        assertTrue(interruptFlag.get());
    }

    @Test
    public void testAwaitTerminationBeforeShutdown() throws InterruptedException {
        ThreadPool pool = new ThreadPool();
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 10000; i++) {
            pool.execute(counter::incrementAndGet);
        }

        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            pool.shutdown();
        }).start();

        pool.awaitTermination();
        assertEquals(counter.get(), 10000);
    }

    @Test
    public void testExpireAwaitTermination() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1);
        AtomicBoolean run = new AtomicBoolean(false);

        pool.execute(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            run.set(true);
        });

        pool.shutdown();
        assertFalse(pool.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse(run.get());
        pool.awaitTermination();
        assertTrue(run.get());
    }

    @Test
    public void testAwaitShutdownEmptyPool() throws InterruptedException {
        ThreadPool pool = new ThreadPool(0, 3, 1, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        pool.execute(counter::incrementAndGet);

        Thread.sleep(5000);
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            pool.shutdown();
        }).start();
        pool.awaitTermination();
        assertTrue(pool.isShutdown());
        assertTrue(pool.isTerminated());
        assertEquals(counter.get(), 1);
    }

    @Test
    public void testJoinShutdownEmptyPool() throws InterruptedException {
        ThreadPool pool = new ThreadPool(0, 3, 1, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        pool.execute(counter::incrementAndGet);

        Thread.sleep(5000);
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            pool.shutdown();
        }).start();
        pool.join();
        assertEquals(counter.get(), 1);
    }

    @Test
    public void testWorkerCountData() {
        ThreadPool.WorkerCountData workerCountData = new ThreadPool.WorkerCountData();

        assertEquals(workerCountData.getTotalWorkerCount(), 0);
        assertEquals(workerCountData.getIdleWorkerCount(), 0);

        workerCountData.incrementBoth();

        assertEquals(workerCountData.getTotalWorkerCount(), 1);
        assertEquals(workerCountData.getIdleWorkerCount(), 1);

        for (int i = 0; i < 10; i++) {
            workerCountData.incrementBoth();
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 11);
        assertEquals(workerCountData.getIdleWorkerCount(), 11);

        for (int i = 0; i < 15; i++) {
            int oldVal = workerCountData.incrementWorkerTotal();
            assertEquals(oldVal, 11 + i);
        }

        for (int i = 0; i < 7; i++) {
            int oldVal = workerCountData.incrementWorkerIdle();
            assertEquals(oldVal, 11 + i);
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 26);
        assertEquals(workerCountData.getIdleWorkerCount(), 18);
        ThreadPool.WorkerCountPair both = workerCountData.getBoth();
        assertEquals(both.getTotalCount(), 26);
        assertEquals(both.getIdleCount(), 18);

        for (int i = 0; i < 5; i++) {
            ThreadPool.WorkerCountPair oldVal = workerCountData.decrementBoth();
            assertEquals(oldVal.getTotalCount(), 26 - i);
            assertEquals(oldVal.getIdleCount(), 18 - i);
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 21);
        assertEquals(workerCountData.getIdleWorkerCount(), 13);

        for (int i = 0; i < 13; i++) {
            workerCountData.decrementWorkerTotal();
        }

        for (int i = 0; i < 4; i++) {
            workerCountData.decrementWorkerIdle();
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 8);
        assertEquals(workerCountData.getIdleWorkerCount(), 9);

        for (int i = 0; i < 456789; i++) {
            workerCountData.incrementWorkerTotal();
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 456797);
        assertEquals(workerCountData.getIdleWorkerCount(), 9);
        ThreadPool.WorkerCountPair both1 = workerCountData.getBoth();
        assertEquals(both1.getTotalCount(), 456797);
        assertEquals(both1.getIdleCount(), 9);

        for (int i = 0; i < 23456; i++) {
            workerCountData.incrementWorkerIdle();
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 456797);
        assertEquals(workerCountData.getIdleWorkerCount(), 23465);

        for (int i = 0; i < 150000; i++) {
            workerCountData.decrementWorkerTotal();
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 306797);
        assertEquals(workerCountData.getIdleWorkerCount(), 23465);

        for (int i = 0; i < 10000; i++) {
            workerCountData.decrementWorkerIdle();
        }

        assertEquals(workerCountData.getTotalWorkerCount(), 306797);
        assertEquals(workerCountData.getIdleWorkerCount(), 13465);
    }

    @Test
    public void testJoinEnqueuedTask() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 50, 20, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 160; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                counter.incrementAndGet();
            });
        }

        Thread.sleep(5000);
        assertEquals(pool.getCurrentWorkerCount(), 50);

        pool.join();
        assertEquals(counter.get(), 160);
        Thread.sleep(21000);
        assertEquals(pool.getCurrentWorkerCount(), 3);
    }

    @Test
    public void testKillAll() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 10, 2, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++) {
            pool.execute(() -> {
                throw new RuntimeException("this is an expected exception to kill this worker");
            });
        }

        pool.join();
        Thread.sleep(5000);
        assertEquals(pool.getCurrentWorkerCount(), 3);
        assertEquals(pool.getIdleWorkerCount(), 3);
    }

    @Test
    public void testKillSome() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 10, 5, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 10; i++) {
            int finalI = i;
            pool.execute(() -> {
                if (finalI < 3 || finalI % 2 == 0) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    counter.incrementAndGet();
                } else {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("this is an expected exception to kill this worker");
                }
            });
        }

        pool.join();
        assertEquals(counter.get(), 6);
        assertEquals(pool.getCurrentWorkerCount(), 10);
        assertEquals(pool.getIdleWorkerCount(), 10);
        Thread.sleep(10000);
        assertEquals(pool.getCurrentWorkerCount(), 3);
        assertEquals(pool.getIdleWorkerCount(), 3);
    }

    @Test
    public void testKillAllCoreThreads() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 3, 1, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                throw new RuntimeException("this is an expected exception to kill this worker");
            });
        }

        pool.join();

        for (int i = 0; i < 10; i++) {
            int finalI = i;
            pool.execute(() -> {
                if (finalI < 3 || finalI % 2 == 0) {
                    counter.incrementAndGet();
                } else {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("this is an expected exception to kill this worker");
                }
            });
        }

        pool.join();
        assertEquals(counter.get(), 6);
        assertEquals(pool.getCurrentWorkerCount(), 3);
        assertEquals(pool.getIdleWorkerCount(), 3);
    }

    @Test
    public void testEmptyCorePool() throws InterruptedException {
        ThreadPool pool = new ThreadPool(0, 3, 5, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 3; i++) {
            pool.execute(counter::incrementAndGet);
        }

        pool.join();
        assertEquals(counter.get(), 3);
        Thread.sleep(6000);
        assertEquals(pool.getCurrentWorkerCount(), 0);

        for (int i = 0; i < 3; i++) {
            pool.execute(counter::incrementAndGet);
        }

        pool.join();
        assertEquals(counter.get(), 6);
    }

    @Test
    public void testSubmit() throws ExecutionException, InterruptedException {
        ThreadPool pool = new ThreadPool(0, 3, 5, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        Future<Integer> fut = pool.submit(() -> {
            counter.incrementAndGet();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return counter.getAndIncrement();
        });

        int result = fut.get();
        assertEquals(result, 1);
    }

    @Test
    public void testMultipleSubmit() throws ExecutionException, InterruptedException {
        ThreadPool pool = new ThreadPool(0, 3, 5, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        Future<Integer> fut1 = pool.submit(() -> {
            for (int i = 0; i < 10000; i++) {
                counter.incrementAndGet();
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < 10000; i++) {
                counter.incrementAndGet();
            }

            return counter.get();
        });

        Future<Integer> fut2 = pool.submit(() -> {
            int result;
            try {
                result = fut1.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            result += 15000;

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            result += 20000;
            return result;
        });

        int result = fut2.get();
        assertEquals(result, 55000);
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testSubmitException() throws ExecutionException, InterruptedException {
        ThreadPool pool = ThreadPool.Builder.create().setCoreSize(5).setMaxSize(50).build();

        Future<Integer> fut = pool.submit(() -> {
            int i = 3;

            //noinspection ConstantConditions
            if (i == 3) {
                throw new RuntimeException("this is an expected exception to kill this worker");
            }

            return i;
        });

        fut.get();
    }

    // test that explicit max size being lower than default core size is handled correctly
    // otherwise would throw on a machine with more than 1 cpu core / hyperthread
    @Test
    public void testBuilderMaxSize() {
        ThreadPool pool = ThreadPool.Builder.create().setMaxSize(1).build();
        assertEquals(pool.getCoreSize(), 1);
    }

    @Test
    public void testMultiThreadJoin() throws InterruptedException {
        ThreadPool pool = new ThreadPool();
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                counter.incrementAndGet();
            });
        }

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            pool.execute(() -> {
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                counter.addAndGet(2);
            });
        });
        thread.start();

        Thread thread1 = new Thread(() -> {
            try {
                pool.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread1.start();
        Thread thread2 = new Thread(() -> {
            try {
                pool.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread2.start();
        Thread thread3 = new Thread(() -> {
            try {
                pool.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread3.start();

        thread1.join();
        thread2.join();
        thread3.join();
        assertEquals(counter.get(), 5);
    }

    @Test
    public void testShutdownNow() throws InterruptedException {
        ThreadPool pool = new ThreadPool(3, 5, 10, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    counter.incrementAndGet();
                }
            });
        }

        // tasks that won't get the chance to run
        Runnable r1 = counter::incrementAndGet;
        Runnable r2 = counter::incrementAndGet;
        Runnable r3 = counter::incrementAndGet;
        pool.execute(r1);
        pool.execute(r2);
        pool.execute(r3);

        Thread.sleep(5000);
        List<Runnable> drain = pool.shutdownNow();
        pool.awaitTermination();

        assertThat(drain).containsExactly(r1, r2, r3);
        assertEquals(counter.get(), 5);
    }

    @Test
    public void testRejectTasksAfterShutdownNow() throws InterruptedException {
        AtomicInteger rejectionCounter = new AtomicInteger(0);
        ThreadPool pool = ThreadPool.Builder.create()
            .setCoreSize(100)
            .setMaxSize(100)
            .setRejectedExecutionHandler((t, p) -> rejectionCounter.incrementAndGet())
            .build();
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 100; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    counter.incrementAndGet();
                }
            });

            if (i == 49) {
                pool.shutdownNow();
            }
        }

        pool.join();
        assertEquals(rejectionCounter.get(), 50);
        assertEquals(counter.get(), 50);
    }

    @Test
    public void testSubmitOnShutdownNow() throws InterruptedException {
        ThreadPool pool = ThreadPool.Builder.create().setRejectedExecutionHandler(RejectedExecutionHandler.DISCARD_POLICY).build();
        AtomicInteger counter = new AtomicInteger(0);

        pool.execute(counter::incrementAndGet);

        Object latch = new Object();

        Thread notifyThread = new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            synchronized (latch) {
                latch.notifyAll();
            }
        });

        Thread submissionThread = new Thread(() -> {
            try {
                synchronized (latch) {
                    latch.wait();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            pool.execute(() -> counter.addAndGet(10));
        });

        pool.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                counter.incrementAndGet();
                return;
            }

            counter.decrementAndGet();
        });

        notifyThread.start();
        submissionThread.start();

        synchronized (latch) {
            latch.wait();
        }
        pool.shutdownNow();
        pool.join();
        assertEquals(counter.get(), 2);
    }

    @Test
    public void testRejectedExecution() throws InterruptedException {
        AtomicInteger rejectionCounter = new AtomicInteger(0);
        ThreadPool pool = ThreadPool.Builder.create()
            .setCoreSize(3)
            .setMaxSize(5)
            .setRejectedExecutionHandler((t, p) -> rejectionCounter.incrementAndGet())
            .setWorkQueue(new SynchronousQueue<>())
            .build();
        AtomicInteger counter = new AtomicInteger(0);

        Object firstExecuteMonitor = new Object();

        for (int i = 0; i < 5; i++) {
            int finalI = i;
            new Thread(() -> {
                for (int j = 0; j < 8; j++) {
                    pool.execute(() -> {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        counter.incrementAndGet();
                    });

                    if (finalI == 0 && j == 0) {
                        synchronized (firstExecuteMonitor) {
                            firstExecuteMonitor.notifyAll();
                        }
                    }
                }
            }).start();
        }

        synchronized (firstExecuteMonitor) {
            firstExecuteMonitor.wait();
        }
        pool.join();
        assertEquals(counter.get(), 5);
        assertEquals(rejectionCounter.get(), 35);
    }

    // provoke a situation where the last thread to become idle does not notify joiners because there are still tasks
    // in the queue but the pool is shutdown before any of those tasks are executed and assert that this does not cause
    // an infinite join
    //
    // this test case is very racy and generally requires to extend the time period the worker requires to poll the next
    // task artificially, which is why this test is disabled by default
    //
    // when adding a sleep statement a the top of the work loop and commenting out the part where joiners are notified
    // after the last worker exits in the corresponding finally block this test should consistently cause an infinite
    // join.
    @Test(enabled = false)
    public void testShutDownNowImmediatelyAfterTaskExecutionWithQueuedTasks() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1, 1);
        AtomicInteger counter = new AtomicInteger(0);
        Object latch = new Object();

        pool.execute(() -> {
            counter.incrementAndGet();

            synchronized (latch) {
                try {
                    latch.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Runnable abandonedTask = counter::incrementAndGet;
        pool.execute(abandonedTask);

        Thread notifyThread = new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            synchronized (latch) {
                latch.notifyAll();
            }
        });

        Thread shutdownThread = new Thread(() -> {
            synchronized (latch) {
                try {
                    latch.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                Thread.sleep(0, 100000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            pool.shutdownNow();
        });

        notifyThread.start();
        shutdownThread.start();

        synchronized (latch) {
            latch.wait();
        }
        pool.join();
        // if the actual value = 2 that means the race condition did not occur and the queued task got to run
        // try adding a sleep statement at the top of the work loop to extend to possible time window for the race condition
        // to occur
        assertEquals(counter.get(), 1);
    }

    // benchmarks / comparisons with java.util.concurrent.ThreadPoolExecutor
    //
    // run in interpreter mode (JVM arg -Xint) for the fairest results

    @Test(enabled = false)
    public void benchLong() {
        ThreadPool pool = new ThreadPool(10, 10, 60, TimeUnit.SECONDS);
        //ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        AtomicLong count = new AtomicLong(0);
        long nanos1 = System.nanoTime();
        for (int i = 0; i < 10000000; i++) {
            pool.execute(count::incrementAndGet);
        }
        long delta1 = System.nanoTime() - nanos1;
        System.out.println("millis pool " + delta1 / 1000000);

    }

    @Test(enabled = false)
    public void bench() {
        ThreadPool pool = new ThreadPool(1, 1, 60, TimeUnit.SECONDS);
        //ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        AtomicLong count = new AtomicLong(0);
        long nanos1 = System.nanoTime();
        pool.execute(count::incrementAndGet);
        long delta1 = System.nanoTime() - nanos1;
        pool.execute(count::incrementAndGet);
        pool.execute(count::incrementAndGet);
        pool.execute(count::incrementAndGet);
        long delta2 = System.nanoTime() - nanos1;
        System.out.println("first execute " + delta1);
        System.out.println("total nanos " + delta2);
    }

    @Test(enabled = false)
    public void benchMedium() {
        ThreadPool pool = new ThreadPool(32, 128, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        //ThreadPoolExecutor pool = new ThreadPoolExecutor(32, 128, 60, TimeUnit.SECONDS, new SynchronousQueue<>());
        AtomicLong count = new AtomicLong(0);
        long nanos = System.nanoTime();
        for (int i = 0; i < 128; i++) {
            pool.execute(count::incrementAndGet);
        }
        long delta = System.nanoTime() - nanos;
        System.out.println("nanos pool " + delta);
    }

    @Test(dataProvider = "executorServiceProvider1", enabled = false)
    public void benchSuppliedLong(ExecutorService pool) throws InterruptedException {
        AtomicLong count = new AtomicLong(0);
        long nanos1 = System.nanoTime();
        for (int i = 0; i < 10000000; i++) {
            pool.execute(count::incrementAndGet);
        }
        long delta1 = System.nanoTime() - nanos1;
        System.out.printf("millis pool %s: %d\n", pool.getClass().getName(), delta1 / 1000000);
        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);
    }

    @DataProvider
    public Object[] executorServiceProvider1() {
        return new Object[]{
            new ThreadPool(10, 10, 60, TimeUnit.SECONDS),
            new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>())
        };
    }

    @Test(dataProvider = "executorServiceProvider2", enabled = false)
    public void benchSupplied(ExecutorService pool) throws InterruptedException {
        AtomicLong count = new AtomicLong(0);
        long nanos1 = System.nanoTime();
        pool.execute(count::incrementAndGet);
        long delta1 = System.nanoTime() - nanos1;
        pool.execute(count::incrementAndGet);
        pool.execute(count::incrementAndGet);
        pool.execute(count::incrementAndGet);
        long delta2 = System.nanoTime() - nanos1;
        System.out.println(pool.getClass().getName() + " first execute " + delta1);
        System.out.println(pool.getClass().getName() + " total nanos " + delta2);
        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);
    }

    @DataProvider
    public Object[] executorServiceProvider2() {
        return new Object[]{
            new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>()),
            new ThreadPool(1, 1, 60, TimeUnit.SECONDS)
        };
    }

}
