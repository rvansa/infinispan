package org.infinispan.commands;

/**
 * Functor that should be executed on the cache entry while holding this entry's lock
 * (therefore, no concurrent operations can be executed meanwhile).
 *
 * In clustered context implementation must be {@link java.io.Serializable}, {@link java.io.Externalizable}
 * or appropriate {@link org.infinispan.commons.marshall.Externalizer} must be registered.
 *
 * This should be a swiss-army knife for executing custom changes on single entry, while minimizing replication costs.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface EntryProcessor<K, V, T> {
   /**
    * Executes the processor logic on this entry. If this method throws an exception,
    * no modification is executed (however, the modification may be executed on other nodes).
    * Also, listeners are invoked only once after this method finishes.
    *
    * @param entry Entry to be read and modified.
    * @param retry If this is set to true, it is possible that this entry processor
    *              has already executed the operation (due to topology change).
    * @return Must not be null.
    */
   T process(MutableEntry<K, V> entry, boolean retry);
}
