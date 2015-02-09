package org.infinispan.commands;

import java.util.Map;

import org.infinispan.container.entries.metadata.MetadataAware;

/**
 * Entry where the user can read old value and metadata, and set new value and metadata.
 * Calling {@link #setValue(Object)}  with null argument will make the entry to be removed.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface MutableEntry<K, V> extends Map.Entry<K, V>, MetadataAware {
}
