package org.infinispan.objects.interceptors;

import java.util.Collection;

import org.infinispan.commands.read.EntryRetrievalCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.EntryProcessCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.Converter;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.objects.CacheObject;

/**
 * This iterator attaches any {@link org.infinispan.objects.CacheObject}
 * to the {@link org.infinispan.manager.EmbeddedCacheManager} running this interceptor stack.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class CacheObjectInterceptor extends BaseCustomInterceptor {

   private Object attach(Object value) {
      if (value instanceof CacheObject) {
         CacheObject cacheObject = (CacheObject) value;
         cacheObject.attachCacheManager(embeddedCacheManager);
      } else if (value instanceof CacheEntry) {
         attach(((CacheEntry) value).getValue());
      } else if (value instanceof Collection) {
         for (Object item : (Collection) value) {
            attach(item);
         }
      }
      return value;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return attach(handleDefault(ctx, command));
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return attach(handleDefault(ctx, command));
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return attach(handleDefault(ctx, command));
   }

   @Override
   public Object visitEntryProcessCommand(InvocationContext ctx, EntryProcessCommand command) throws Throwable {
      return attach(handleDefault(ctx, command));
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return attach(handleDefault(ctx, command));
   }

   @Override
   public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
      return attach(handleDefault(ctx, command));
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return attach(handleDefault(ctx, command));
   }

   @Override
   public Object visitEntryRetrievalCommand(InvocationContext ctx, EntryRetrievalCommand command) throws Throwable {
      EntryIterable iterable = (EntryIterable) handleDefault(ctx, command);
      return iterable != null ? new AttachingIterable(iterable) : null;
   }

   private class AttachingIterable implements EntryIterable {
      final EntryIterable iterable;

      private AttachingIterable(EntryIterable iterable) {
         this.iterable = iterable;
      }

      @Override
      public CloseableIterable<CacheEntry> converter(Converter converter) {
         final CloseableIterable<CacheEntry> iterable = this.iterable.converter(converter);
         return new CloseableIterable<CacheEntry>() {
            @Override
            public void close() {
               iterable.close();
            }

            @Override
            public CloseableIterator<CacheEntry> iterator() {
               return new AttachingIterator(iterable.iterator());
            }
         };
      }

      @Override
      public void close() {
         iterable.close();
      }

      @Override
      public CloseableIterator iterator() {
         return new AttachingIterator(iterable.iterator());
      }
   }

   private class AttachingIterator implements CloseableIterator {
      private final CloseableIterator iterator;

      private AttachingIterator(CloseableIterator iterator) {
         this.iterator = iterator;
      }

      @Override
      public void close() {
         iterator.close();
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public Object next() {
         return attach(iterator.next());
      }

      @Override
      public void remove() {
         iterator.remove();
      }
   };
}
