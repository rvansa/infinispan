package org.infinispan.objects;

import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.objects.impl.SharedLongConditionalImpl;
import org.infinispan.objects.impl.SharedLongDeltaImpl;

/**
 * Class that should be used to create shared objects.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Lookup {
   private static final String DEFAULT_SHARED_LONG_IMPL = SharedLongImplementation.DELTA.name();
   private enum SharedLongImplementation {
      CONDITIONAL,
      DELTA
   }

   private final static SharedLongImplementation sharedLongImplementation
      = SharedLongImplementation.valueOf(System.getProperty("org.infinispan.objects.sharedlong", DEFAULT_SHARED_LONG_IMPL));

   static {
      LogFactory.getLog(Lookup.class).info("Using shared long implementation: " + sharedLongImplementation);
   }

   public static <K> SharedLong createSharedLong(Cache<K, ?> cache, K key, long initialValue) {
      switch (sharedLongImplementation) {
         case CONDITIONAL:
            return new SharedLongConditionalImpl(cache, key, initialValue, false);
         case DELTA:
            return new SharedLongDeltaImpl(cache, key, initialValue, false);
         default:
            throw new IllegalStateException();
      }
   }

   public static <K> SharedLong lookupSharedLong(Cache<K, ?> cache, K key) {
      switch (sharedLongImplementation) {
         case CONDITIONAL:
            return new SharedLongConditionalImpl(cache, key);
         case DELTA:
            return new SharedLongDeltaImpl(cache, key);
         default:
            throw new IllegalStateException();
      }
   }

   public static <K> SharedLong lookupOrCreateSharedLong(Cache<K, ?> cache, K key, long initialValue) {
      switch (sharedLongImplementation) {
         case CONDITIONAL:
            return new SharedLongConditionalImpl(cache, key, initialValue, true);
         case DELTA:
            return new SharedLongDeltaImpl(cache, key, initialValue, true);
         default:
            throw new IllegalStateException();
      }
   }
}
