package org.infinispan.remoting.transport.tcp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The idea is that if someone is actively waiting for the future to complete
 * (being blocked in {@link #get()} or {@link #get(long, TimeUnit)}, we can let him do some work
 * when this future is about to be completed. This is achieved by calling {@link #lazyComplete(Consumer, Executor)}.
 * The provided consumer won't be executed in the one calling {@link #lazyComplete(Consumer, Executor)},
 * but either in any thread calling {@link #get()}/{@link #get(long, TimeUnit)} or in the executor.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RelayingCompletableFuture<T> extends CompletableFuture<T> {
   protected final Sync sync;

   protected static class Sync {
      protected final Runnable[] runnables;
      protected int retrieving;

      public Sync(int capacity) {
         this.runnables = new Runnable[capacity];
      }
   }

   protected RelayingCompletableFuture(Sync sync) {
      this.sync = sync;
   }

   public RelayingCompletableFuture(int maxActions) {
      this.sync = new Sync(maxActions);
   }

   @Override
   public boolean complete(T value) {
      try {
         return super.complete(value);
      } finally {
         synchronized (sync) {
            sync.notifyAll();
         }
      }
   }

   @Override
   public boolean completeExceptionally(Throwable ex) {
      try {
         return super.completeExceptionally(ex);
      } finally {
         synchronized (sync) {
            sync.notifyAll();
         }
      }
   }

   @Override
   public <U> RelayingCompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
      RelayingCompletableFuture<U> other = new RelayingCompletableFuture<>(sync);
      super.whenComplete((v, t) -> {
         try {
            if (t == null) {
               other.complete(fn.apply(v));
            } else {
               other.completeExceptionally(t);
            }
         } catch (Throwable t2) {
            other.completeExceptionally(t2);
         }
      });
      return other;
   }

   @Override
   public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
      RelayingCompletableFuture<U> other = new RelayingCompletableFuture<>(sync);
      super.whenComplete((v, t) -> {
         try {
            other.complete(fn.apply(v, t));
         } catch (Throwable t2) {
            if (t == null) {
               other.completeExceptionally(t2);
            } else {
               t2.addSuppressed(t);
               other.completeExceptionally(t);
            }
         }
      });
      return other;
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      for (;;) {
         if (isDone()) {
            return super.get();
         } else {
            synchronized (sync) {
               if (isDone()) {
                  return super.get();
               }
               ++sync.retrieving;
               try {
                  sync.wait();
                  executeRunnables();
               } finally {
                  --sync.retrieving;
               }
            }
         }
      }
   }

   private void executeRunnables() {
      if (!isDone()) {
         Runnable[] runnables = sync.runnables;
         for (int i = 0; i < runnables.length; i++) {
            Runnable r = runnables[i];
            if (r != null) {
               runnables[i] = null;
               r.run();
            }
         }
      }
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      for (;;) {
         if (isDone()) {
            return super.get();
         } else {
            synchronized (sync) {
               if (isDone()) {
                  return super.get();
               }
               ++sync.retrieving;
               try {
                  long now = System.nanoTime();
                  if (now < deadline) {
                     long diff = deadline - now;
                     sync.wait(TimeUnit.NANOSECONDS.toMillis(diff), (int) (diff % TimeUnit.MILLISECONDS.toNanos(1)));
                  } else {
                     throw new TimeoutException();
                  }
                  executeRunnables();
               } finally {
                  --sync.retrieving;
               }
            }
         }
      }
   }
}
