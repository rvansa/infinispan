package org.infinispan.functional.impl;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Status;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.infinispan.functional.impl.Params.withFuture;

/**
 * Abstract functional map, providing implementations for some of the shared methods.
 *
 * @since 8.0
 */
abstract class AbstractFunctionalMap<K, V> implements FunctionalMap<K, V> {

   protected final FunctionalMapImpl<K, V> fmap;
   protected final boolean autoCommit;
   protected final TransactionManager transactionManager;
   protected final BatchContainer batchContainer;

   protected AbstractFunctionalMap(FunctionalMapImpl<K, V> fmap) {
      this.fmap = fmap;
      TransactionConfiguration transactionConfiguration = fmap.cache.getCacheConfiguration().transaction();
      this.autoCommit = transactionConfiguration.transactionMode().isTransactional() && transactionConfiguration.autoCommit();
      this.transactionManager = fmap.cache.getTransactionManager();
      this.batchContainer = fmap.cache.getCacheConfiguration().invocationBatching().enabled() ? fmap.cache.getBatchContainer() : null;
   }

   @Override
   public String getName() {
      return "";
   }

   @Override
   public Status getStatus() {
      return fmap.getStatus();
   }

   @Override
   public void close() throws Exception {
      fmap.close();
   }

   protected Transaction getOngoingTransaction() {
      try {
         Transaction transaction = null;
         if (transactionManager != null) {
            transaction = transactionManager.getTransaction();
            if (transaction == null && batchContainer != null) {
               transaction = batchContainer.getBatchTransaction();
            }
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   protected <R> Traversable<R> invokeWithImplicitTransaction(VisitableCommand cmd, int keyCount) {
      final InvocationContext ctx;
      boolean implicitTransaction = false;
      if (autoCommit) {
         Transaction transaction = getOngoingTransaction();
         if (transaction == null) {
            try {
               transactionManager.begin();
               transaction = transactionManager.getTransaction();
            } catch (RuntimeException e) {
               throw e;
            } catch (Exception e) {
               throw new CacheException(e);
            }
            implicitTransaction = true;
         }
         ctx = fmap.invCtxFactory().createInvocationContext(transaction, implicitTransaction);
      } else {
         ctx = fmap.invCtxFactory().createInvocationContext(true, keyCount);
      }
      Traversable<R> traversable = null;
      try {
         return traversable = Traversables.of(((List<R>) fmap.chain().invoke(ctx, cmd)).stream());
      } catch (Exception ex) {
         if (implicitTransaction) {
            try {
               transactionManager.rollback();
            } catch (RuntimeException e) {
               e.addSuppressed(ex);
               throw e;
            } catch (Exception e) {
               e.addSuppressed(ex);
               throw new CacheException(e);
            }
         }
         throw ex;
      } finally {
         if (traversable != null && implicitTransaction) {
            try {
               transactionManager.commit();
            } catch (RuntimeException e) {
               throw e;
            } catch (Exception e) {
               throw new CacheException(e);
            }
         }
      }
   }

   protected <R> CompletableFuture<R> invokeWithImplicitTransaction(VisitableCommand cmd, Param<Param.FutureMode> futureMode, int keyCount, boolean returnVoid) {
      final InvocationContext ctx;
      boolean implicitTransaction = false;
      boolean suspended = false;
      if (autoCommit) {
         Transaction transaction = getOngoingTransaction();
         if (transaction == null) {
            try {
               transactionManager.begin();
               switch (futureMode.get()) {
                  case COMPLETED:
                     transaction = transactionManager.getTransaction();
                     break;
                  case ASYNC:
                     // we pass the transaction in invocation context and don't want to have it associated with the thread
                     transaction = transactionManager.suspend();
                     break;
                  default:
                     throw new IllegalArgumentException();
               }
            } catch (RuntimeException e) {
               throw e;
            } catch (Exception e) {
               throw new CacheException(e);
            }
            implicitTransaction = true;
         }
         ctx = fmap.invCtxFactory().createInvocationContext(transaction, implicitTransaction);
      } else {
         ctx = fmap.invCtxFactory().createInvocationContext(true, keyCount);
      }
      if (cmd instanceof AbstractDataWriteCommand) {
         ctx.setLockOwner(((AbstractDataWriteCommand) cmd).getKeyLockOwner());
      }
      final boolean resume = implicitTransaction && suspended;
      CompletableFuture<R> future = withFuture(futureMode, fmap.asyncExec(), () -> {
         if (resume) {
            try {
               // TODO: Resuming transaction in different thread is supported in JBossTM,
               // but other TMs may not support it
               transactionManager.resume(((TxInvocationContext) ctx).getTransaction());
            } catch (RuntimeException e) {
               throw e;
            } catch (Exception e) {
               throw new CacheException(e);
            }
         }
         Object retval = fmap.chain().invoke(ctx, cmd);
         if (returnVoid) {
            return null;
         } else {
            return (R) retval;
         }
      });
      if (implicitTransaction) {
         return future.whenComplete((retVal, throwable) -> {
            try {
               if (throwable == null) {
                  transactionManager.commit();
               } else {
                  transactionManager.rollback();
               }
            } catch (RuntimeException e) {
               throw e;
            } catch (Exception e) {
               throw new CacheException(e);
            }
         });
      }
      return future;
   }
}
