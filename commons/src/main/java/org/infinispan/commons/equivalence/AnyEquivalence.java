package org.infinispan.commons.equivalence;

/**
 * A compare function for objects.
 *
 * @author Galder Zamarreño
 * @since 5.3
 * @deprecated
 */
public final class AnyEquivalence<T> implements Equivalence<T> {

   private static final AnyEquivalence<Object> INSTANCE = new AnyEquivalence<>();

   public static final AnyEquivalence<String> STRING = getInstance();

   public static final AnyEquivalence<Byte> BYTE = getInstance();

   public static final AnyEquivalence<Short> SHORT = getInstance();

   public static final AnyEquivalence<Integer> INT = getInstance();

   public static final AnyEquivalence<Long> LONG = getInstance();

   public static final AnyEquivalence<Double> DOUBLE  = getInstance();

   public static final AnyEquivalence<Float> FLOAT = getInstance();

   public static final AnyEquivalence<Boolean> BOOLEAN = getInstance();

   // To avoid instantiation
   private AnyEquivalence() {
   }

   @Override
   public int hashCode(Object obj) {
      return obj == null ? 0 : obj.hashCode();
   }

   @Override
   public boolean equals(T obj, Object otherObj) {
      return obj != null ? obj.equals(otherObj) : otherObj == null;
   }

   @Override
   public String toString(Object obj) {
      return String.valueOf(obj);
   }

   @Override
   public boolean isComparable(Object obj) {
      return obj instanceof Comparable;
   }

   @Override
   @SuppressWarnings("unchecked")
   public int compare(T obj, T otherObj) {
      return ((Comparable<T>) obj).compareTo(otherObj);
   }

   @SuppressWarnings("unchecked")
   public static <T> AnyEquivalence<T> getInstance() {
      return (AnyEquivalence<T>) INSTANCE;
   }

   //todo [anistor] unused
   @SuppressWarnings("unchecked")
   public static <T> AnyEquivalence<T> getInstance(Class<T> classType) {
      return (AnyEquivalence<T>) INSTANCE;
   }
}
