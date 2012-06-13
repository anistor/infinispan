/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.context.Flag.SKIP_LOCKING;
import static org.testng.AssertJUnit.*;


@Test(groups = "functional", testName = "atomic.AtomicMapFunctionalTest")
public class AtomicMapFunctionalTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AtomicMapFunctionalTest.class);
   private Cache<String, Object> cache;
   private TransactionManager tm;
   private EmbeddedCacheManager cm;

   @BeforeMethod
   public void setUp() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable();

      cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .locking().lockAcquisitionTimeout(200).writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ);

      cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .transaction().lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(200)
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .build();

      cm = TestCacheManagerFactory.createCacheManager(cb);

      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cache = null;
      tm = null;
   }

   public void testChangesOnAtomicMap() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testTxChangesOnAtomicMap() throws Exception {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      tm.begin();
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm.suspend();

      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a") == null;

      tm.resume(t);
      tm.commit();

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocks() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
//      InvocationContextContainer icc = TestingUtil.extractComponent(cache, InvocationContextContainer.class);
//      InvocationContext ic = icc.createInvocationContext(false, -1);
//      ic.setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
//      assert icc.getInvocationContext(true).hasFlag(SKIP_LOCKING);
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testTxChangesOnAtomicMapNoLocks() throws Exception {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      tm.begin();
      assert map.isEmpty();
//      TestingUtil.extractComponent(cache, InvocationContextContainer.class).createInvocationContext(true, -1).setFlags(SKIP_LOCKING);
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm.suspend();

      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a") == null;

      tm.resume(t);
      tm.commit();

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocksExistingData() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
      map.put("x", "y");
      assert map.get("x").equals("y");
//      TestingUtil.extractComponent(cache, InvocationContextContainer.class).createInvocationContext(false, -1).setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");
      assert map.get("x").equals("y");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("x").equals("y");
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testRemovalOfAtomicMap() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      map.put("hello", "world");

      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();

      map = AtomicMapLookup.getAtomicMap(cache, "key");
      map.put("hello2", "world2");
      assertEquals(2, map.size());
      AtomicMapLookup.removeAtomicMap(cache, "key");
      assertFalse(cache.containsKey("key"));

      // check if map is valid before commit
      try {
         map.size(); // this must fail, map is stale

         fail("Stale AtomicMap reference was not detected.");
      } catch (IllegalStateException e) {
         assertTrue(e.getMessage().contains("key has been concurrently removed"));
      }

      tm.commit();

      // check if map is valid after commit
      try {
         map.size(); // this must fail

         fail("Stale AtomicMap reference was not detected.");
      } catch (IllegalStateException e) {
         assertTrue(e.getMessage().contains("key has been concurrently removed"));
      }

      // this should re-create an empty map
      AtomicMap<String, String> map2 = AtomicMapLookup.getAtomicMap(cache, "key");
      assertTrue(map2.isEmpty());
   }

   public void testConcurrentRemovalOfAtomicMap() throws InterruptedException {
      final AtomicMap<String, String> map0 = AtomicMapLookup.getAtomicMap(cache, "key");
      map0.put("k1", "v1");
      map0.put("k2", "v2");

      final AtomicInteger rolledBack = new AtomicInteger();
      final CountDownLatch latch1 = new CountDownLatch(1);
      final CountDownLatch latch2 = new CountDownLatch(1);
      final CountDownLatch latch3 = new CountDownLatch(1);

      Thread t1 = new Thread(new Runnable() {
         public void run() {
            try {
               latch1.await();

               tm.begin();
               try {
                  AtomicMap<String, String> map1 = AtomicMapLookup.getAtomicMap(cache, "key");
                  assertEquals(2, map1.size());
                  assertEquals("v1", map1.get("k1"));
                  assertEquals("v2", map1.get("k2"));
                  latch2.countDown();
                  latch3.await();
                  assertEquals(2, map1.size());
                  map1.put("k3","v3");
                  tm.commit();
               } catch (Exception e) {
                  if (e instanceof RollbackException) {
                     rolledBack.incrementAndGet();
                  }

                  // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                  if (tm.getTransaction() != null) {
                     try {
                        tm.rollback();
                        rolledBack.incrementAndGet();
                     } catch (SystemException e1) {
                        log.error("Failed to rollback", e1);
                     }
                  }
                  throw e;
               }
            } catch (Exception ex) {
               ex.printStackTrace();
            }
         }
      }, "NodeMoveAPITest.Remover-1");

      Thread t2 = new Thread(new Runnable() {
         public void run() {
            try {
               latch2.await();

               tm.begin();
               try {
                  AtomicMap<String, String> map2 = AtomicMapLookup.getAtomicMap(cache, "key");
                  assertEquals(2, map2.size());
                  AtomicMapLookup.removeAtomicMap(cache, "key");
                  tm.commit();
                  latch3.countDown();
               } catch (Exception e) {
                  if (e instanceof RollbackException) {
                     rolledBack.incrementAndGet();
                  }

                  // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                  if (tm.getTransaction() != null) {
                     try {
                        tm.rollback();
                        rolledBack.incrementAndGet();
                     } catch (SystemException e1) {
                        log.error("Failed to rollback", e1);
                     }
                  }
                  throw e;
               }
            } catch (Exception ex) {
               ex.printStackTrace();
            }
         }
      }, "NodeMoveAPITest.Remover-2");

      t1.start();
      t2.start();
      latch1.countDown();
      t1.join();
      t2.join();

      assertFalse(cache.containsKey("key"));
      assertEquals(0, rolledBack.get());
   }

   public void testConcurrentRemoveAndUpdateOfAtomicMap() throws InterruptedException {
      final AtomicMap<String, String> map0 = AtomicMapLookup.getAtomicMap(cache, "key");
      map0.put("k1", "v1");
      map0.put("k2", "v2");

      final AtomicInteger rolledBack = new AtomicInteger();
      final CountDownLatch latch1 = new CountDownLatch(1);
      final CountDownLatch latch2 = new CountDownLatch(1);
      final CountDownLatch latch3 = new CountDownLatch(1);

      Thread t1 = new Thread(new Runnable() {
         public void run() {
            try {
               latch1.await();

               tm.begin();
               try {
                  AtomicMap<String, String> map1 = AtomicMapLookup.getAtomicMap(cache, "key");
                  assertEquals(2, map1.size());
                  assertEquals("v1", map1.get("k1"));
                  assertEquals("v2", map1.get("k2"));
                  latch2.countDown();
                  latch3.await();
                  assertEquals(2, map1.size());
                  tm.commit();
               } catch (Exception e) {
                  if (e instanceof RollbackException) {
                     rolledBack.incrementAndGet();
                  } else if (tm.getTransaction() != null) {
                     // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                     try {
                        tm.rollback();
                        rolledBack.incrementAndGet();
                     } catch (SystemException e1) {
                        log.error("Failed to rollback", e1);
                     }
                  }
                  throw e;
               }
            } catch (Exception ex) {
               ex.printStackTrace();
            }
         }
      }, "NodeMoveAPITest.Remover-1");

      Thread t2 = new Thread(new Runnable() {
         public void run() {
            try {
               latch2.await();

               tm.begin();
               try {
                  AtomicMap<String, String> map2 = AtomicMapLookup.getAtomicMap(cache, "key");
                  assertEquals(2, map2.size());
                  AtomicMapLookup.removeAtomicMap(cache, "key");
                  tm.commit();
                  latch3.countDown();
               } catch (Exception e) {
                  if (e instanceof RollbackException) {
                     rolledBack.incrementAndGet();
                  } else if (tm.getTransaction() != null) {
                     // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                     try {
                        tm.rollback();
                        rolledBack.incrementAndGet();
                     } catch (SystemException e1) {
                        log.error("Failed to rollback", e1);
                     }
                  }
                  throw e;
               }
            } catch (Exception ex) {
               ex.printStackTrace();
            }
         }
      }, "NodeMoveAPITest.Remover-2");

      t1.start();
      t2.start();
      latch1.countDown();
      t1.join();
      t2.join();
   }
}
