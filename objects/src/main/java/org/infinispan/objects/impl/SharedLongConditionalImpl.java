package org.infinispan.objects.impl;

import org.infinispan.Cache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.objects.SharedLong;

/**
 * Shared long implemented using conditional operations on the cache.
 * Alternative implementation is {@link org.infinispan.objects.impl.SharedLongDeltaImpl}
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SharedLongConditionalImpl extends CacheObjectImpl<Long> implements SharedLong {
   private static final Log log = LogFactory.getLog(SharedLong.class);
   private static final boolean trace = log.isTraceEnabled();

   public SharedLongConditionalImpl() {
      // marshalling only
   }

   public SharedLongConditionalImpl(Cache cache, Object key, long value, boolean allowExisting) {
      super(cache, key);
      Long previous = (Long) cache.putIfAbsent(key, value);
      if (!allowExisting) {
         assertNull(cache, key, previous);
      }
   }

   public SharedLongConditionalImpl(Cache cache, Object key) {
      super(cache, key);
   }

   @Override
   public long get() {
      Long previous = getCache().get(getKey());
      assertInstance(previous, Long.class);
      return previous;
   }

   @Override
   public void set(long newValue) {
      getCache().getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(getKey(), newValue);
      if (trace) {
         log.tracef("Updated [%s:%s]: ? -> %d", getCacheName(), getKey(), newValue);
      }
   }

   @Override
   public long getAndSet(long newValue) {
      Long previous = getCache().put(getKey(), newValue);
      assertInstance(previous, Long.class);
      if (trace) {
         log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), previous, newValue);
      }
      return previous;
   }

   @Override
   public boolean compareAndSet(long expect, long update) {
      boolean replaced = getCache().replace(getKey(), expect, update);
      if (trace) {
         if (replaced)
            log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), expect, update);
         else
            log.tracef("Update failed [%s:%s]: %d -> %d", getCacheName(), getKey(), expect, update);
      }
      return replaced;
   }

   @Override
   public long getAndIncrement() {
      return getAndAdd(1);
   }

   @Override
   public long getAndDecrement() {
      return getAndAdd(-1);
   }

   @Override
   public long getAndAdd(long delta) {
      Cache<Object, Long> cache = getCache();
      Long previous;
      long next;
      // TODO: will this work with pessimistic locking?
      do {
         previous = cache.get(getKey());
         assertInstance(previous, Long.class);
         next = previous + delta;
      } while (!cache.replace(getKey(), previous, next));
      if (trace) {
         log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), previous, next);
      }
      return previous;
   }

   @Override
   public long incrementAndGet() {
      return addAndGet(1);
   }

   @Override
   public long decrementAndGet() {
      return addAndGet(-1);
   }

   @Override
   public long addAndGet(long delta) {
      Cache<Object, Long> cache = getCache();
      Long previous;
      long next;
      // TODO: will this work with pessimistic locking?
      do {
         previous = cache.get(getKey());
         assertInstance(previous, Long.class);
         next = previous + delta;
      } while (!cache.replace(getKey(), previous, next));
      if (trace) {
         log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), previous, next);
      }
      return next;
   }
}
