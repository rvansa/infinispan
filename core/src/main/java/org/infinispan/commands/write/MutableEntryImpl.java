package org.infinispan.commands.write;

import org.infinispan.commands.MutableEntry;
import org.infinispan.metadata.Metadata;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class MutableEntryImpl<K, V> implements MutableEntry<K, V> {
   private final K key;
   private V value;
   private Metadata metadata;
   private boolean valueSet;
   private boolean metadataSet;

   public MutableEntryImpl(K key, V value, Metadata metadata) {
      this.key = key;
      this.value = value;
      this.metadata = metadata;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      return value;
   }

   @Override
   public V setValue(V value) {
      V tmp = this.value;
      this.value = value;
      this.valueSet = true;
      return tmp;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
      this.metadataSet = true;
   }

   public boolean isMetadataSet() {
      return metadataSet;
   }
}
