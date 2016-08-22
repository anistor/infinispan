package org.infinispan.query.dsl.impl;

import static org.junit.Assert.assertEquals;

import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class CreationTest {

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   @Test
   public void testWithDifferentFactory1() {
      QueryFactory qf1 = new DummyQueryFactory();
      QueryFactory qf2 = new DummyQueryFactory();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014809: The given condition was created by another factory");

      qf1.from("MyDummyType")
            .not(qf2.having("attr1").eq("1")); // exception expected
   }

   @Test
   public void testWithDifferentFactory2() {
      QueryFactory qf1 = new DummyQueryFactory();
      QueryFactory qf2 = new DummyQueryFactory();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014809: The given condition was created by another factory");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .and(qf2.having("attr2").eq("2")); // exception expected
   }

   @Test
   public void testWithDifferentFactory3() {
      QueryFactory qf1 = new DummyQueryFactory();
      QueryFactory qf2 = new DummyQueryFactory();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("ISPN014809: The given condition was created by another factory");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .or(qf2.having("attr2").eq("2")); // exception expected
   }

   @Test
   public void testWithDifferentBuilder1() {
      QueryFactory qf1 = new DummyQueryFactory();

      FilterConditionContext fcc = qf1.having("attr1").eq("1");

      Query q1 = qf1.from("MyDummyType")
            .not(fcc)
            .build();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("The given condition is already in use by another builder");

      qf1.from("MyDummyType")
            .not(fcc);    // exception expected
   }

   @Test
   public void testWithDifferentBuilder2() {
      QueryFactory qf1 = new DummyQueryFactory();

      FilterConditionContext fcc = qf1.having("attr1").eq("1");

      Query q1 = qf1.from("MyDummyType")
            .not(fcc)
            .build();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("The given condition is already in use by another builder");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .and(fcc);    // exception expected
   }

   @Test
   public void testWithDifferentBuilder3() {
      QueryFactory qf1 = new DummyQueryFactory();

      FilterConditionContext fcc = qf1.having("attr1").eq("1");

      Query q1 = qf1.from("MyDummyType")
            .not(fcc)
            .build();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("The given condition is already in use by another builder");

      qf1.from("MyDummyType")
            .having("attr1").eq("1")
            .or(fcc);    // exception expected
   }

   @Test
   public void testEQ() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .having("f1").eq("X")
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE _gen0.f1 = 'X'", q.getQueryString());
   }

   @Test
   public void testNot() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .not().having("f1").eq("X")
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE _gen0.f1 != 'X'", q.getQueryString());
   }

   @Test
   public void testAnd1() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .having("f1").eq("X").and().having("f2").eq("Y")
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE (_gen0.f1 = 'X') AND (_gen0.f2 = 'Y')", q.getQueryString());
   }

   @Test
   public void testAnd2() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .having("f1").eq("X").and(qf.having("f2").eq("Y"))
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE (_gen0.f1 = 'X') AND (_gen0.f2 = 'Y')", q.getQueryString());
   }

   @Test
   public void testOr1() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .having("f1").eq("X").or().having("f2").eq("Y")
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE (_gen0.f1 = 'X') OR (_gen0.f2 = 'Y')", q.getQueryString());
   }

   @Test
   public void testOr2() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .having("f1").eq("X").or(qf.having("f2").eq("Y"))
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE (_gen0.f1 = 'X') OR (_gen0.f2 = 'Y')", q.getQueryString());
   }

   @Test
   public void testXor1() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .having("f1").eq("X").xor().having("f2").eq("Y")
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE (_gen0.f1 = 'X') AND NOT (_gen0.f2 = 'Y') OR NOT (_gen0.f1 = 'X') AND (_gen0.f2 = 'Y')", q.getQueryString());
   }

   @Test
   public void testXor2() {
      DummyQueryFactory qf = new DummyQueryFactory();

      DummyQuery q = (DummyQuery) qf.from("TestEntity")
            .having("f1").eq("X").xor(qf.having("f2").eq("Y"))
            .toBuilder().build();

      assertEquals("FROM TestEntity _gen0 WHERE (_gen0.f1 = 'X') AND NOT (_gen0.f2 = 'Y') OR NOT (_gen0.f1 = 'X') AND (_gen0.f2 = 'Y')", q.getQueryString());
   }
}
