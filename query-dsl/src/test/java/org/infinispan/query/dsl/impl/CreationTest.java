package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.QueryFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test misuse of sub-queries from other factories or builders during creation.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class CreationTest {

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   protected QueryFactory getQueryFactory() {
      return new DummyQueryFactory();
   }

   @Test
   public void testWithDifferentFactory1() {
      QueryFactory qf1 = getQueryFactory();
      QueryFactory qf2 = getQueryFactory();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014809: The given condition was created by another factory");

      qf1.from("MyDummyType")
            .not(qf2.having("attr1").eq("1")); // exception expected
   }

   @Test
   public void testWithDifferentFactory2() {
      QueryFactory qf1 = getQueryFactory();
      QueryFactory qf2 = getQueryFactory();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014809: The given condition was created by another factory");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .and(qf2.having("attr2").eq("2")); // exception expected
   }

   @Test
   public void testWithDifferentFactory3() {
      QueryFactory qf1 = getQueryFactory();
      QueryFactory qf2 = getQueryFactory();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014809: The given condition was created by another factory");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .or(qf2.having("attr2").eq("2")); // exception expected
   }

   @Test
   public void testWithDifferentBuilder1() {
      QueryFactory qf1 = getQueryFactory();

      FilterConditionContext fcc = qf1.having("attr1").eq("1");

      qf1.from("MyDummyType")
            .not(fcc)
            .build();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014810: The given condition is already in use by another builder");

      qf1.from("MyDummyType")
            .not(fcc);    // exception expected
   }

   @Test
   public void testWithDifferentBuilder2() {
      QueryFactory qf1 = getQueryFactory();

      FilterConditionContext fcc = qf1.having("attr1").eq("1");

      qf1.from("MyDummyType")
            .not(fcc)
            .build();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014810: The given condition is already in use by another builder");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .and(fcc);    // exception expected
   }

   @Test
   public void testWithDifferentBuilder3() {
      QueryFactory qf1 = getQueryFactory();

      FilterConditionContext fcc = qf1.having("attr1").eq("1");

      qf1.from("MyDummyType")
            .not(fcc)
            .build();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014810: The given condition is already in use by another builder");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .or(fcc);    // exception expected
   }
}
