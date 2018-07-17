package org.infinispan.query.dsl.impl;

import java.util.Arrays;
import java.util.Collection;

import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class OperatorAndArgument<ArgumentType> implements Visitable {

   private static final Log log = Logger.getMessageLogger(Log.class, OperatorAndArgument.class.getName());

   protected final AttributeCondition attributeCondition;

   protected final ArgumentType argument;

   protected OperatorAndArgument(AttributeCondition attributeCondition, ArgumentType argument) {
      if (attributeCondition == null) {
         throw log.argumentCannotBeNull("attributeCondition");
      }
      this.attributeCondition = attributeCondition;
      this.argument = argument;
   }

   AttributeCondition getAttributeCondition() {
      return attributeCondition;
   }

   /**
    * Returns the argument.
    */
   ArgumentType getArgument() {
      return argument;
   }

   /**
    * 'Casts' the argument to an Iterable if possible.
    */
   Iterable<?> getIterableArgument() {
      if (argument instanceof Collection) {
         return (Collection) argument;
      } else if (argument instanceof Object[]) {
         return Arrays.asList((Object[]) argument);
      }
      throw log.expectingCollectionOrArray();
   }

   //todo [anistor] would be nice to also validate that the argument type is compatible with the operator for early error detection, but this will be detected during query string parsing anyway
   void validate() {
      if (argument == null) {
         throw log.argumentCannotBeNull();
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "(" + argument + ')';
   }
}
