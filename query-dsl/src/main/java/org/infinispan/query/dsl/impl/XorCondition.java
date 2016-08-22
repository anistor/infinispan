package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
class XorCondition extends BooleanCondition {

   public XorCondition(QueryFactory queryFactory, BaseCondition leftCondition, BaseCondition rightCondition) {
      super(queryFactory, leftCondition, rightCondition);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "(" + getFirstCondition() + ") XOR (" + getSecondCondition() + ")";
   }
}
