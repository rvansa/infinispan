package org.infinispan.objects;


import org.infinispan.manager.EmbeddedCacheManager;

/**
 * An object that wraps another 'holder' stored in cache.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface CacheObject {
      /**
    * This method should be called after object instantiation or unmarshalling.
    *
    * @param cacheManager Set the manager which should be used to retrieve the 'holder'
    * @throws java.lang.IllegalStateException if the object is already attached to different cache manager
    */
   void attachCacheManager(EmbeddedCacheManager cacheManager);

   /**
    * @return True if this instance was already attached to a cache manager
    */
   boolean isCacheManagerAttached();
}
