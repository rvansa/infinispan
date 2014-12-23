package org.infinispan.objects.impl;

import java.util.UUID;

import org.infinispan.objects.KeyFactory;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class UUIDKeyFactory implements KeyFactory<UUID> {
   @Override
   public UUID generateKey() {
      return UUID.randomUUID();
   }
}
