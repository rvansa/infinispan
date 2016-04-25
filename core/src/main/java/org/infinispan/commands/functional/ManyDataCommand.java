package org.infinispan.commands.functional;

import java.util.Set;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface ManyDataCommand<K> {
   Set<? extends K> getKeys();
}
