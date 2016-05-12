package org.infinispan.remoting.transport.tcp;

import java.util.concurrent.Executor;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class LazyCompletableFuture<T> extends RelayingCompletableFuture<T> {

   public LazyCompletableFuture() {
      super(new Sync(1));
   }

   public LazyCompletableFuture(RelayingCompletableFuture<?> other) {
      super(other.sync);
   }

   public void lazyComplete(Runnable runnable, Executor executor) {
      synchronized (sync) {
         int free = -1;
         for (int i = 0; i < sync.runnables.length; ++i) {
            if (sync.runnables[i] == null) {
               free = i;
               break;
            }
         }
         if (free < 0) {
            throw new IllegalStateException();
         } else {
            sync.runnables[free] = runnable;
         }
         if (sync.retrieving == 0) {
            executor.execute(() -> {
               synchronized (sync) {
                  for (int i = 0; i < sync.runnables.length; ++i) {
                     Runnable r = sync.runnables[i];
                     if (r != null) {
                        sync.runnables[i] = null;
                        r.run();
                     }
                  }
                  // in case somebody started waiting after lazyComplete has been called
                  sync.notifyAll();
               }
            });
         } else {
            // the other party will do the job for us
            sync.notifyAll();
         }
      }
   }


}
