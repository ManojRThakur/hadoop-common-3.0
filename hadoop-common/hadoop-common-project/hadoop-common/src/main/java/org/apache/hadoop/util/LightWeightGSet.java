/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.PrintStream;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * A low memory footprint {@link GSet} implementation,
 * which uses an array for storing the elements
 * and linked lists for collision resolution.
 *
 * No rehash will be performed.
 * Therefore, the internal array will never be resized.
 *
 * This class does not support null element.
 *
 * This class is not thread safe.
 *
 * @param <K> Key type for looking up the elements
 * @param <E> Element type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link LinkedElement} interface.
 */
@InterfaceAudience.Private
public class LightWeightGSet<@Tainted K extends java.lang.@Tainted Object, @Tainted E extends @Tainted K> implements @Tainted GSet<K, E> {
  /**
   * Elements of {@link LightWeightGSet}.
   */
  public static interface LinkedElement {
    /** Set the next element. */
    public void setNext(LightWeightGSet.@Tainted LinkedElement this, @Tainted LinkedElement next);

    /** Get the next element. */
    public @Tainted LinkedElement getNext(LightWeightGSet.@Tainted LinkedElement this);
  }

  static final @Tainted int MAX_ARRAY_LENGTH = 1 << 30; //prevent int overflow problem
  static final @Tainted int MIN_ARRAY_LENGTH = 1;

  /**
   * An internal array of entries, which are the rows of the hash table.
   * The size must be a power of two.
   */
  private final @Tainted LinkedElement @Tainted [] entries;
  /** A mask for computing the array index from the hash value of an element. */
  private final @Tainted int hash_mask;
  /** The size of the set (not the entry array). */
  private @Tainted int size = 0;
  /** Modification version for fail-fast.
   * @see ConcurrentModificationException
   */
  private @Tainted int modification = 0;

  /**
   * @param recommended_length Recommended size of the internal array.
   */
  public @Tainted LightWeightGSet(final @Tainted int recommended_length) {
    final @Tainted int actual = actualArrayLength(recommended_length);
    if (LOG.isDebugEnabled()) {
      LOG.debug("recommended=" + recommended_length + ", actual=" + actual);
    }
    entries = new @Tainted LinkedElement @Tainted [actual];
    hash_mask = entries.length - 1;
  }

  //compute actual length
  private static @Tainted int actualArrayLength(@Tainted int recommended) {
    if (recommended > MAX_ARRAY_LENGTH) {
      return MAX_ARRAY_LENGTH;
    } else if (recommended < MIN_ARRAY_LENGTH) {
      return MIN_ARRAY_LENGTH;
    } else {
      final @Tainted int a = Integer.highestOneBit(recommended);
      return a == recommended? a: a << 1;
    }
  }

  @Override
  public @Tainted int size(@Tainted LightWeightGSet<K, E> this) {
    return size;
  }

  private @Tainted int getIndex(@Tainted LightWeightGSet<K, E> this, final @Tainted K key) {
    return key.hashCode() & hash_mask;
  }

  private @Tainted E convert(@Tainted LightWeightGSet<K, E> this, final @Tainted LinkedElement e){
    @SuppressWarnings("unchecked")
    final E r = (E)e;
    return r;
  }

  @Override
  public @Tainted E get(@Tainted LightWeightGSet<K, E> this, final @Tainted K key) {
    //validate key
    if (key == null) {
      throw new @Tainted NullPointerException("key == null");
    }

    //find element
    final @Tainted int index = getIndex(key);
    for(@Tainted LinkedElement e = entries[index]; e != null; e = e.getNext()) {
      if (e.equals(key)) {
        return convert(e);
      }
    }
    //element not found
    return null;
  }

  @Override
  public @Tainted boolean contains(@Tainted LightWeightGSet<K, E> this, final @Tainted K key) {
    return get(key) != null;
  }

  @Override
  public @Tainted E put(@Tainted LightWeightGSet<K, E> this, final @Tainted E element) {
    //validate element
    if (element == null) {
      throw new @Tainted NullPointerException("Null element is not supported.");
    }
    if (!(element instanceof @Tainted LinkedElement)) {
      throw new @Tainted HadoopIllegalArgumentException(
          "!(element instanceof LinkedElement), element.getClass()="
          + element.getClass());
    }
    final @Tainted LinkedElement e = (@Tainted LinkedElement)element;

    //find index
    final @Tainted int index = getIndex(element);

    //remove if it already exists
    final E existing = remove(index, element);

    //insert the element to the head of the linked list
    modification++;
    size++;
    e.setNext(entries[index]);
    entries[index] = e;

    return existing;
  }

  /**
   * Remove the element corresponding to the key,
   * given key.hashCode() == index.
   *
   * @return If such element exists, return it.
   *         Otherwise, return null.
   */
  private @Tainted E remove(@Tainted LightWeightGSet<K, E> this, final @Tainted int index, final @Tainted K key) {
    if (entries[index] == null) {
      return null;
    } else if (entries[index].equals(key)) {
      //remove the head of the linked list
      modification++;
      size--;
      final @Tainted LinkedElement e = entries[index];
      entries[index] = e.getNext();
      e.setNext(null);
      return convert(e);
    } else {
      //head != null and key is not equal to head
      //search the element
      @Tainted
      LinkedElement prev = entries[index];
      for(@Tainted LinkedElement curr = prev.getNext(); curr != null; ) {
        if (curr.equals(key)) {
          //found the element, remove it
          modification++;
          size--;
          prev.setNext(curr.getNext());
          curr.setNext(null);
          return convert(curr);
        } else {
          prev = curr;
          curr = curr.getNext();
        }
      }
      //element not found
      return null;
    }
  }

  @Override
  public @Tainted E remove(@Tainted LightWeightGSet<K, E> this, final @Tainted K key) {
    //validate key
    if (key == null) {
      throw new @Tainted NullPointerException("key == null");
    }
    return remove(getIndex(key), key);
  }

  @Override
  public @Tainted Iterator<@Tainted E> iterator(@Tainted LightWeightGSet<K, E> this) {
    return new @Tainted SetIterator();
  }

  @Override
  public @Tainted String toString(@Tainted LightWeightGSet<K, E> this) {
    final @Tainted StringBuilder b = new @Tainted StringBuilder(getClass().getSimpleName());
    b.append("(size=").append(size)
     .append(String.format(", %08x", hash_mask))
     .append(", modification=").append(modification)
     .append(", entries.length=").append(entries.length)
     .append(")");
    return b.toString();
  }

  /** Print detailed information of this object. */
  public void printDetails(@Tainted LightWeightGSet<K, E> this, final @Tainted PrintStream out) {
    out.print(this + ", entries = [");
    for(@Tainted int i = 0; i < entries.length; i++) {
      if (entries[i] != null) {
        @Tainted
        LinkedElement e = entries[i];
        out.print("\n  " + i + ": " + e);
        for(e = e.getNext(); e != null; e = e.getNext()) {
          out.print(" -> " + e);
        }
      }
    }
    out.println("\n]");
  }

  private class SetIterator implements @Tainted Iterator<E> {
    /** The starting modification for fail-fast. */
    private final @Tainted int startModification = modification;
    /** The current index of the entry array. */
    private @Tainted int index = -1;
    /** The next element to return. */
    private @Tainted LinkedElement next = nextNonemptyEntry();

    /** Find the next nonempty entry starting at (index + 1). */
    private @Tainted LinkedElement nextNonemptyEntry(@Tainted LightWeightGSet<K, E>.SetIterator this) {
      for(index++; index < entries.length && entries[index] == null; index++);
      return index < entries.length? entries[index]: null;
    }

    @Override
    public @Tainted boolean hasNext(@Tainted LightWeightGSet<K, E>.SetIterator this) {
      return next != null;
    }

    @Override
    public @Tainted E next(@Tainted LightWeightGSet<K, E>.SetIterator this) {
      if (modification != startModification) {
        throw new @Tainted ConcurrentModificationException("modification=" + modification
            + " != startModification = " + startModification);
      }

      final E e = convert(next);

      //find the next element
      final @Tainted LinkedElement n = next.getNext();
      next = n != null? n: nextNonemptyEntry();

      return e;
    }

    @Override
    public void remove(@Tainted LightWeightGSet<K, E>.SetIterator this) {
      throw new @Tainted UnsupportedOperationException("Remove is not supported.");
    }
  }
  
  /**
   * Let t = percentage of max memory.
   * Let e = round(log_2 t).
   * Then, we choose capacity = 2^e/(size of reference),
   * unless it is outside the close interval [1, 2^30].
   */
  public static @Tainted int computeCapacity(@Tainted double percentage, @Tainted String mapName) {
    return computeCapacity(Runtime.getRuntime().maxMemory(), percentage,
        mapName);
  }
  
  @VisibleForTesting
  static @Tainted int computeCapacity(@Tainted long maxMemory, @Tainted double percentage,
      @Tainted
      String mapName) {
    if (percentage > 100.0 || percentage < 0.0) {
      throw new @Tainted HadoopIllegalArgumentException("Percentage " + percentage
          + " must be greater than or equal to 0 "
          + " and less than or equal to 100");
    }
    if (maxMemory < 0) {
      throw new @Tainted HadoopIllegalArgumentException("Memory " + maxMemory
          + " must be greater than or equal to 0");
    }
    if (percentage == 0.0 || maxMemory == 0) {
      return 0;
    }
    //VM detection
    //See http://java.sun.com/docs/hotspot/HotSpotFAQ.html#64bit_detection
    final @Tainted String vmBit = System.getProperty("sun.arch.data.model");

    //Percentage of max memory
    final @Tainted double percentDivisor = 100.0/percentage;
    final @Tainted double percentMemory = maxMemory/percentDivisor;
    
    //compute capacity
    final @Tainted int e1 = (@Tainted int)(Math.log(percentMemory)/Math.log(2.0) + 0.5);
    final @Tainted int e2 = e1 - ("32".equals(vmBit)? 2: 3);
    final @Tainted int exponent = e2 < 0? 0: e2 > 30? 30: e2;
    final @Tainted int c = 1 << exponent;

    LOG.info("Computing capacity for map " + mapName);
    LOG.info("VM type       = " + vmBit + "-bit");
    LOG.info(percentage + "% max memory = "
        + StringUtils.TraditionalBinaryPrefix.long2String(maxMemory, "B", 1));
    LOG.info("capacity      = 2^" + exponent + " = " + c + " entries");
    return c;
  }
  
  public void clear(@Tainted LightWeightGSet<K, E> this) {
    for (@Tainted int i = 0; i < entries.length; i++) {
      entries[i] = null;
    }
    size = 0;
  }
}
