package org.infinispan.objects.impl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.objects.SharedLong;

/**
 * Uses delta-aware holder (instead of plain long) to store the value.
 * Therefore, all methods can be implemented using single
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SharedLongDeltaImpl extends CacheObjectImpl<SharedLongDeltaImpl.DeltaAwareLong> implements SharedLong {
   private static final Log log = LogFactory.getLog(SharedLong.class);
   private static final boolean trace = log.isTraceEnabled();

   public SharedLongDeltaImpl() {
      // marshalling only
   }

   public SharedLongDeltaImpl(Cache cache, Object key, long value, boolean allowExisting) {
      super(cache, key);
      DeltaAwareLong previous = (DeltaAwareLong) cache.putIfAbsent(key, new DeltaAwareLong(value, false));
      if (!allowExisting) {
         assertNull(cache, key, previous);
      }
   }

   public SharedLongDeltaImpl(Cache cache, Object key) {
      super(cache, key);
   }

   @Override
   public long get() {
      DeltaAwareLong previous = getCache().get(getKey());
      assertInstance(previous, DeltaAwareLong.class);
      return previous.value;
   }

   @Override
   public void set(long newValue) {
      getCache().getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(getKey(), new DeltaAwareLong(newValue, false));
      if (trace) {
         log.tracef("Updated [%s:%s]: ? -> %d", getCacheName(), getKey(), newValue);
      }
   }

   @Override
   public long getAndSet(long newValue) {
      DeltaAwareLong previous = getCache().put(getKey(), new DeltaAwareLong(newValue, false));
      assertInstance(previous, DeltaAwareLong.class);
      if (trace) {
         log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), previous.value, newValue);
      }
      return previous.value;
   }

   @Override
   public boolean compareAndSet(long expect, long update) {
      boolean replaced = getCache().replace(getKey(), new DeltaAwareLong(expect, false), new DeltaAwareLong(update, false));
      if (trace) {
         if (replaced) {
            log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), expect, update);
         } else {
            log.tracef("Failed to update [%s:%s]: %d -> %d", getCacheName(), getKey(), expect, update);
         }
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
      DeltaAwareLong previous = getCache().put(getKey(), new DeltaAwareLong(delta, true));
      assertInstance(previous, DeltaAwareLong.class);
      if (trace) {
         log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), previous.value, previous.value + delta);
      }
      return previous.value;
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
      DeltaAwareLong previous = getCache().put(getKey(), new DeltaAwareLong(delta, true));
      assertInstance(previous, DeltaAwareLong.class);
      if (trace) {
         log.tracef("Updated [%s:%s]: %d -> %d", getCacheName(), getKey(), previous.value, previous.value + delta);
      }
      return previous.value + delta;
   }

   /**
    * This works as both value holder, delta generator and the delta itself, due to the
    * fact that when originator == primary owner, the delta is not explicitly fetched
    */
   public static class DeltaAwareLong implements DeltaAware, Delta, Externalizable {
      private long value;
      private boolean relative;

      public DeltaAwareLong() {
      }

      public DeltaAwareLong(long value, boolean relative) {
         this.value = value;
         this.relative = relative;
      }

      @Override
      public Delta delta() {
         return this;
      }

      @Override
      public void commit() {
      }

      @Override
      public DeltaAware merge(DeltaAware d) {
         if (trace) {
            log.tracef("Merging %s with %s", d, this);
         }
         if (d instanceof DeltaAwareLong) {
            long newValue = relative ? ((DeltaAwareLong) d).value + value : value;
            return new DeltaAwareLong(newValue, false);
         } else if (d == null && !relative) {
            return new DeltaAwareLong(value, false);
         } else {
            throw new IllegalArgumentException("Cannot merge " + d);
         }
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeLong(value);
         out.writeBoolean(relative);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         value = in.readLong();
         relative = in.readBoolean();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         DeltaAwareLong that = (DeltaAwareLong) o;

         if (value != that.value) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return (int) (value ^ (value >>> 32));
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("DeltaAwareLong{");
         sb.append("value=").append(value);
         sb.append(", relative=").append(relative);
         sb.append('}');
         return sb.toString();
      }
   }
}
