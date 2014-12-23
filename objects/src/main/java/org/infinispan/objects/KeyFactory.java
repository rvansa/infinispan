package org.infinispan.objects;

/**
 * Generates random keys for internal use of cache objects
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface KeyFactory<K> {
   K generateKey();
}
