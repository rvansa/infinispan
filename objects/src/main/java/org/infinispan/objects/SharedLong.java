package org.infinispan.objects;

/**
 * Interface partially copied from {@link java.util.concurrent.atomic.AtomicLong}
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface SharedLong {
   /**
    * Gets the current value.
    *
    * @return the current value
    */
   long get();

   /**
    * Sets to the given value.
    *
    * @param newValue the new value
    */
   void set(long newValue);

   /**
    * Atomically sets to the given value and returns the old value.
    *
    * @param newValue the new value
    * @return the previous value
    */
   long getAndSet(long newValue);

   /**
    * Atomically sets the value to the given updated value
    * if the current value {@code ==} the expected value.
    *
    * @param expect the expected value
    * @param update the new value
    * @return true if successful. False return indicates that
    * the actual value was not equal to the expected value.
    */
   boolean compareAndSet(long expect, long update);

   /**
    * Atomically increments by one the current value.
    *
    * @return the previous value
    */
   long getAndIncrement();

   /**
    * Atomically decrements by one the current value.
    *
    * @return the previous value
    */
   long getAndDecrement();

   /**
    * Atomically adds the given value to the current value.
    *
    * @param delta the value to add
    * @return the previous value
    */
   long getAndAdd(long delta);

   /**
    * Atomically increments by one the current value.
    *
    * @return the updated value
    */
   long incrementAndGet();

   /**
    * Atomically decrements by one the current value.
    *
    * @return the updated value
    */
   long decrementAndGet();

   /**
    * Atomically adds the given value to the current value.
    *
    * @param delta the value to add
    * @return the updated value
    */
   long addAndGet(long delta);
}
