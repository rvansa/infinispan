package org.infinispan.objects;

import java.util.Collection;
import java.util.ListIterator;

/**
 * List that is shared in Cache, safe for concurrent access.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface SharedAppendOnlyList<T> extends Collection<T> {
   T get(int index);

   int indexOf(Object o);

   int lastIndexOf(Object o);

   ListIterator<T> listIteratorAtEnd();

   ListIterator<T> listIterator();

   ListIterator<T> listIterator(int index);
}
