package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AddressHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author anistor@redhat.com
 * @since 8.2
 */
@Test(groups = "profiling", testName = "query.dsl.embedded.impl.AggregationProfilingTest")
public class AggregationProfilingTest extends MultipleCacheManagersTest {

   private final int NUM_NODES = 10;

   private final int NUM_OWNERS = 3;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = buildConfiguration();
      createCluster(c, NUM_NODES);
      waitForClusterToForm();
   }

   private ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      c.clustering().hash().numOwners(NUM_OWNERS);
      return c;
   }

   public void testAggregationPerformance() {
      final int numEntries = 100000;

      for (int i = 0; i < numEntries; i++) {
         User u = makeUser(i);
         Cache<Integer, User> cache = cache(i % NUM_NODES);
         cache.put(u.getId(), u);
      }

      QueryFactory qf = Search.getQueryFactory(cache(0));
      for (int i = 0; i < 1000000000; i++) {
         Query q1 = qf.from(UserHS.class)
               .select(Expression.property("name"), Expression.property("age"))
               .having("age").gte(18)
               .toBuilder().build();
         Query q2 = qf.from(UserHS.class)
               .select(Expression.property("name"), Expression.avg("age"))
               .having("age").gte(18)
               .toBuilder().groupBy("name")
               .build();

         long t1 = System.nanoTime();
         q1.list();
         long t2 = System.nanoTime();
         q2.list();
         long t3 = System.nanoTime();

         long d1 = t2 - t1;
         long d2 = t3 - t2;

         System.out.printf("AggregationProfilingTest.testAggregationPerformance took %d ms %d ms\n", d1 / 1000000, d2 / 1000000);
      }
   }

   private User makeUser(int i) {
      User user = new UserHS();
      user.setId(i);
      user.setName("John");
      user.setSurname("Doe");
      user.setGender(User.Gender.MALE);
      user.setAge(22);
      user.setAccountIds(new HashSet<Integer>(Arrays.asList(1, 2)));
      user.setNotes("Lorem ipsum dolor sit amet");

      Address address1 = new AddressHS();
      address1.setStreet("Main Street");
      address1.setPostCode("X1234");
      address1.setNumber(156);

      Address address2 = new AddressHS();
      address2.setStreet("Old Street");
      address2.setPostCode("ZZ");
      address2.setNumber(156);
      user.setAddresses(Arrays.asList(address1, address2));
      return user;
   }
}
