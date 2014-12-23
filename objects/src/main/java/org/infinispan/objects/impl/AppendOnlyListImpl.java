package org.infinispan.objects.impl;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.infinispan.Cache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.objects.KeyFactory;
import org.infinispan.objects.SharedAppendOnlyList;

/**
 * Implementation of shared append-only list.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AppendOnlyListImpl<T> extends CacheObjectImpl<Object> implements SharedAppendOnlyList<T> {
   private static final Log log = LogFactory.getLog(SharedAppendOnlyList.class);
   private KeyFactory keyFactory = new UUIDKeyFactory(); // TODO configurable

   public AppendOnlyListImpl() {
      // marshalling only
   }

   public AppendOnlyListImpl(Cache cache, Object key, int maxFragmentSize, boolean allowExisting) {
      super(cache, key);
      ListInfo previous = (ListInfo) cache.putIfAbsent(key, new ListInfo(maxFragmentSize));
      if (!allowExisting) {
         assertNull(cache, key, previous);
      }
   }

   public AppendOnlyListImpl(Cache cache, Object key) {
      super(cache, key);
   }



   @Override
   public int size() {
      ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
      Fragment<T> lastFragment = (Fragment<T>) getCache().get(info.getLastFragment());
      return size(info, lastFragment);
   }

   private int size(ListInfo<T> info, Fragment<T> lastFragment) {
      return (info.fragments.length - 1) * info.maxFragmentSize + (lastFragment == null ? 0 : lastFragment.elements.length);
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public boolean contains(Object o) {
      if (o == null) throw new NullPointerException();
      for (T item : this) {
         if (item.equals(o)) return true;
      }
      return false;
   }

   @Override
   public Iterator<T> iterator() {
      return listIterator();
   }

   @Override
   public Object[] toArray() {
      ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
      if (info.fragments.length == 0) return new Object[] {};
      Fragment<T> lastFragment = (Fragment<T>) getCache().get(info.getLastFragment());
      Object[] a = new Object[size(info, lastFragment)];
      fillArray(a, info, lastFragment);
      return a;
   }

   @Override
   public <T1> T1[] toArray(T1[] a) {
      Class<?> componentType = a.getClass().getComponentType();
      ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
      if (info.fragments.length == 0) {
         if (a == null) {
            return (T1[]) Array.newInstance(componentType, 0);
         } else {
            if (a.length > 0) a[0] = null;
            return a;
         }
      }
      Fragment<T> lastFragment = (Fragment<T>) getCache().get(info.getLastFragment());
      int size = size(info, lastFragment);
      if (size > a.length) {
         a = (T1[]) Array.newInstance(componentType, size);
      }
      fillArray(a, info, lastFragment);
      return a;
   }

   private <T1> void fillArray(T1[] a, ListInfo<T> info, Fragment<T> lastFragment) {
      int index = 0;
      for (int fragmentId = 0; fragmentId < info.fragments.length - 1; ++fragmentId) {
         Fragment<T> fragment = (Fragment<T>) getCache().get(info.fragments[fragmentId]);
         if (fragment == null) {
            // the fragment is not the last one and other fragments must not be null
            throw new IllegalStateException();
         }
         for (Object item : fragment.elements) {
            if (index >= a.length) return;
            a[index++] = (T1) item;
         }
      }
      if (lastFragment != null) {
         for (Object item : lastFragment.elements) {
            if (index >= a.length) return;
            a[index++] = (T1) item;
         }
      }
      if (index < a.length) a[index] = null;
   }

   @Override
   public boolean add(T item) {
      if (item == null) throw new NullPointerException();
      outerLoop: for (; ; ) {
         ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
         Object fragmentKey = info.getLastFragment();
         Fragment<T> fragment = null;
         if (fragmentKey != null) {
            fragment = (Fragment<T>) getCache().get(fragmentKey);
            if (fragment == null) {
               // fragment not created yet though it's already registered,
               // we will compete for that - that makes this algorithm wait-free
               fragment = (Fragment<T>) getCache().putIfAbsent(fragmentKey, new Fragment<>(item));
               if (fragment == null) {
                  return true;
               }
            }
         }
         innerLoop: for (;;) {
            if (fragment == null || fragment.elements.length >= info.maxFragmentSize) {
               fragmentKey = keyFactory.generateKey();
               if (!getCache().replace(getKey(), info, info.appendFragment(fragmentKey))) {
                  continue outerLoop;
               }
               Fragment<T> nextFragment = new Fragment<>(item);
               fragment = (Fragment<T>) getCache().putIfAbsent(fragmentKey, nextFragment);
               if (fragment == null) {
                  return true;
               }
            } else {
               Fragment<T> updatedFragment = fragment.appendItem(item);
               if (getCache().replace(fragmentKey, fragment, updatedFragment)) {
                  return true;
               } else {
                  fragment = (Fragment<T>) getCache().get(fragmentKey);
               }
            }
         }
      }
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (c.isEmpty()) return true;
      HashSet<?> set = new HashSet<>(c);
      for (T item : this) {
         if (set.remove(item) && set.isEmpty()) return true;
      }
      if (log.isTraceEnabled()) {
         log.tracef("%d elements haven't been found in this list: %s", set.size(), set);
      }
      return false;
   }

   @Override
   public boolean addAll(Collection<? extends T> c) {
      if (c == null) throw new NullPointerException();
      if (c.isEmpty()) return false;
      Deque<T> items = new ArrayDeque<>(c);
      ListInfo<T> info = null;
      outerLoop: for (; ; ) {
         if (info == null) {
            info = (ListInfo<T>) getCache().get(getKey());
         }
         Object fragmentKey = info.getLastFragment();
         Fragment<T> fragment = null;
         if (fragmentKey != null) {
            fragment = (Fragment<T>) getCache().get(fragmentKey);
            if (fragment == null) {
               // fragment not created yet though it's already registered,
               // we will compete for that - that makes this algorithm wait-free
               Object[] elements = pollToArray(items, info.maxFragmentSize);
               fragment = (Fragment<T>) getCache().putIfAbsent(fragmentKey, new Fragment<>(elements));
               if (fragment == null) {
                  if (items.isEmpty()) {
                     return true;
                  }
               } else {
                  returnToDeque(items, elements, 0);
               }
            }
         }
         innerLoop: for (;;) {
            if (fragment == null || fragment.elements.length >= info.maxFragmentSize) {
               fragmentKey = keyFactory.generateKey();
               ListInfo<T> newInfo = info.appendFragment(fragmentKey);
               if (!getCache().replace(getKey(), info, newInfo)) {
                  info = null;
                  continue outerLoop;
               }
               info = newInfo;
               Object[] elements = pollToArray(items, info.maxFragmentSize);
               Fragment<T> nextFragment = new Fragment<>(elements);
               fragment = (Fragment<T>) getCache().putIfAbsent(fragmentKey, nextFragment);
               if (fragment == null) {
                  if (items.isEmpty()) {
                     return true;
                  }
               } else {
                  returnToDeque(items, elements, 0);
               }
            } else {
               Object[] updatedElements = Arrays.copyOf(fragment.elements, Math.min(info.maxFragmentSize, fragment.elements.length + items.size()));
               for (int i = fragment.elements.length; i < updatedElements.length; ++i) {
                  updatedElements[i] = items.poll();
               }
               Fragment<T> updatedFragment = new Fragment<T>(updatedElements);
               if (getCache().replace(fragmentKey, fragment, updatedFragment)) {
                  if (items.isEmpty()) {
                     return true;
                  }
               } else {
                  returnToDeque(items, updatedElements, fragment.elements.length);
                  fragment = (Fragment<T>) getCache().get(fragmentKey);
               }
            }
         }
      }
   }

   private void returnToDeque(Deque<T> items, Object[] elements, int firstElement) {
      for (int i = elements.length - 1; i >= firstElement; --i) {
         items.addFirst((T) elements[i]);
      }
   }

   private Object[] pollToArray(Deque<T> items, int maxItems) {
      Object[] elements = new Object[Math.min(maxItems, items.size())];
      for (int i = 0; i < elements.length; ++i) {
         elements[i] = items.poll();
         assert elements[i] != null;
      }
      return elements;
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
   }

   @Override
   public T get(int index) {
      ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
      int fragmentId = index / info.maxFragmentSize;
      if (index < 0 || fragmentId >= info.fragments.length) {
         throw new IndexOutOfBoundsException(outOfBoundsMsg(index, info));
      }
      Fragment<T> fragment = (Fragment<T>) getCache().get(info.fragments[fragmentId]);
      if (fragment == null) {
         // the fragment has not been uploaded yet
         throw new IndexOutOfBoundsException(outOfBoundsMsg(index, info));
      }
      int subindex = index % info.maxFragmentSize;
      if (subindex > fragment.elements.length) {
         throw new IndexOutOfBoundsException(outOfBoundsMsg(index, info, fragment));
      }
      return fragment.get(subindex);
   }

   private String outOfBoundsMsg(int index, ListInfo<T> info) {
      return String.format("Index: %d, Fragments: %d, MaxSize: %d",
            index, info.fragments.length, info.fragments.length * info.maxFragmentSize);
   }

   private String outOfBoundsMsg(int index, ListInfo<T> info, Fragment<T> fragment) {
      return String.format("Index: %d, LastFragmentSize: %d, Size: %d",
            index, fragment.elements.length, (info.fragments.length - 1) * info.maxFragmentSize + fragment.elements.length);
   }

   @Override
   public int indexOf(Object o) {
      if (o == null) throw new NullPointerException();
      int index = 0;
      for (T item: this) {
         if (o.equals(item)) {
            return index;
         }
         ++index;
      }
      return -1;
   }

   @Override
   public int lastIndexOf(Object o) {
      if (o == null) throw new NullPointerException();
            for (ListIterator it = listIteratorAtEnd(); it.hasPrevious(); ) {
         T item = it.previous();
         if (o.equals(item)) {
            return it.previousIndex() + 1;
         }
      }
      return -1;
   }

   @Override
   public ListIterator listIteratorAtEnd() {
      ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
      Object lastFragmentKey = info.getLastFragment();
      Fragment<T> lastFragment = lastFragmentKey == null ? null : (Fragment<T>) getCache().get(lastFragmentKey);
      return new ListIterator(info, size(info, lastFragment), lastFragment, info.fragments.length - 1);
   }

   @Override
   public ListIterator listIterator() {
      ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
      return new ListIterator(info, -1, null, -1);
   }

   @Override
   public ListIterator listIterator(int index) {
      if (index < 0) throw new IndexOutOfBoundsException("Index: " + index);
      ListInfo<T> info = (ListInfo<T>) getCache().get(getKey());
      return new ListIterator(info, index - 1, null, -1);
   }

   protected class ListIterator implements java.util.ListIterator<T> {
      private final ListInfo<T> info;
      private int index;
      private Fragment<T> cachedFragment;
      private int cachedFragmentId;

      public ListIterator(ListInfo<T> info, int index, Fragment<T> cachedFragment, int cachedFragmentId) {
         this.info = info;
         this.index = index;
         this.cachedFragment = cachedFragment;
         this.cachedFragmentId = cachedFragmentId;
      }

      @Override
      public boolean hasNext() {
         int fragmentId = (index + 1) / info.maxFragmentSize;
         int subindex = (index + 1) % info.maxFragmentSize;
         if (fragmentId >= info.fragments.length) return false;
         Fragment<T> fragment = getFragment(fragmentId);
         return fragment != null && subindex < fragment.elements.length;
      }

      private Fragment<T> getFragment(int fragmentId) {
         if (cachedFragmentId != fragmentId) {
            cachedFragment = (Fragment<T>) getCache().get(info.fragments[fragmentId]);
            cachedFragmentId = fragmentId;
         }
         return cachedFragment;
      }


      @Override
      public T next() {
         index++;
         return current();
      }

      @Override
      public boolean hasPrevious() {
         return index > 0;
      }

      @Override
      public T previous() {
         index--;
         if (index < 0) {
            throw new NoSuchElementException();
         }
         return current();
      }

      private T current() {
         int fragmentId = index / info.maxFragmentSize;
         int subindex = index % info.maxFragmentSize;
         if (fragmentId >= info.fragments.length) {
            throw new NoSuchElementException();
         }
         Fragment<T> fragment = getFragment(fragmentId);
         if (fragment != null && subindex < fragment.elements.length) {
            return fragment.get(subindex);
         } else {
            throw new NoSuchElementException();
         }
      }

      @Override
      public int nextIndex() {
         return index + 1;
      }

      @Override
      public int previousIndex() {
         return index - 1;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void set(T t) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void add(T t) {
         throw new UnsupportedOperationException();
      }
   }

   protected static class ListInfo<T> implements Serializable {
      private static final Object[] NO_FRAGMENTS = new Object[0];
      private int maxFragmentSize;
      private Object[] fragments = NO_FRAGMENTS;

      public ListInfo(int maxFragmentSize) {
         this.maxFragmentSize = maxFragmentSize;
      }

      public Object getLastFragment() {
         if (fragments.length == 0) return null;
         return fragments[fragments.length - 1];
      }

      public ListInfo<T> appendFragment(Object fragmentKey) {
         ListInfo<T> copy = new ListInfo<T>(maxFragmentSize);
         copy.fragments = Arrays.copyOf(this.fragments, this.fragments.length + 1);
         copy.fragments[this.fragments.length] = fragmentKey;
         return copy;
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("ListInfo{");
         sb.append("maxFragmentSize=").append(maxFragmentSize);
         sb.append(", numFragments=").append(fragments.length);
         sb.append(", lastFragment=").append(getLastFragment());
         sb.append('}');
         return sb.toString();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         ListInfo listInfo = (ListInfo) o;

         if (maxFragmentSize != listInfo.maxFragmentSize) return false;
         if (!Arrays.deepEquals(fragments, listInfo.fragments)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         // TODO: cache hash code?
         int result = maxFragmentSize;
         result = 31 * result + Arrays.deepHashCode(fragments);
         return result;
      }
   }

   protected static class Fragment<T> implements Serializable  {
      private Object[] elements;

      public Fragment(T item) {
         elements = (T[]) Array.newInstance(item.getClass(), 1);
         elements[0] = item;
      }

      public Fragment(Object[] elements) {
         this.elements = elements;
      }

      public T get(int i) {
         return (T) elements[i];
      }

      public Fragment<T> appendItem(T item) {
         Object[] newElements = Arrays.copyOf(elements, elements.length + 1);
         newElements[elements.length] = item;
         return new Fragment<>(newElements);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Fragment fragment = (Fragment) o;

         if (!Arrays.deepEquals(elements, fragment.elements)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         // TODO: cache hash code?
         return Arrays.deepHashCode(elements);
      }

      @Override
      public String toString() {
         return Arrays.toString(elements);
      }
   }
}
