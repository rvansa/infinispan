package org.infinispan.objects.impl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objects.CacheObject;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class CacheObjectImpl<T> implements CacheObject, Externalizable {
   private String cacheName;
   private Object key;
   private volatile EmbeddedCacheManager manager;
   private volatile Cache cache;

   protected CacheObjectImpl() {
      // marshalling only
   }

   public CacheObjectImpl(Cache cache, Object key) {
      this.cacheName = cache.getName();
      this.key = key;
      this.manager = cache.getCacheManager();
      this.cache = cache;
   }

   /**
    * @return Cache in which the 'holder' instance is stored.
    */
   protected String getCacheName() {
      return cacheName;
   }

   /**
    * @return Key under which the 'holder' instance is stored.
    */
   protected Object getKey() {
      return key;
   }

   protected Cache<Object, T> getCache() {
      if (manager == null) {
         throw new IllegalStateException("Manager not attached");
      }
      Cache c = cache;
      if (c == null) {
         cache = c = manager.getCache(cacheName);
      }
      return c;
   }

   @Override
   public synchronized void attachCacheManager(EmbeddedCacheManager cacheManager) {
      if (manager != null && manager != cacheManager) {
         throw new IllegalStateException("Already attached to " + this.manager);
      }
      this.manager = cacheManager;
   }

   @Override
   public synchronized boolean isCacheManagerAttached() {
      return manager != null;
   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(cacheName);
      out.writeObject(key);
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      cacheName = (String) in.readObject();
      key = (String) in.readObject();
   }

   protected void assertNull(Cache cache, Object key, T previous) {
      if (previous != null) {
         throw new IllegalStateException(getClass().getSimpleName() + " using key " + key + " already exists in cache " + cache.getName() + "!");
      }
   }

   protected void assertInstance(T previous, Class<T> clazz) {
      if (previous == null) {
         throw new IllegalStateException(getClass().getSimpleName() + " using key " + getKey()
               + " was not found in cache " + getCacheName() + "!");
      } else if (!clazz.isInstance(previous)) {
         throw new IllegalStateException(getClass().getSimpleName() + " using key " + getKey()
               + " in cache " + getCacheName() + " has incompatible value: " + previous + "!");
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof CacheObjectImpl)) return false;

      CacheObjectImpl that = (CacheObjectImpl) o;

      if (!cacheName.equals(that.cacheName)) return false;
      if (!key.equals(that.key)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = cacheName.hashCode();
      result = 31 * result + key.hashCode();
      return result;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append("{cacheName=").append(cacheName).append(", key=").append(key).append('}');
      return sb.toString();
   }
}
