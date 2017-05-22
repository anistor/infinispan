package org.infinispan.server.test.query;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.cs.custom.CustomCacheStoreIT;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@RunWith(Arquillian.class)
@Category(Task.class)   //todo [anistor]
public class ClassRegistryIT {

   private static final Log log = LogFactory.getLog(CustomCacheStoreIT.class);

   private static final String ENTITIES_JAR = "test-entities.jar";

   @InfinispanResource("standalone-classregistry")
   RemoteInfinispanServer server;

   protected final String cacheName = "testcache";

   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected RemoteCacheManagerFactory rcmFactory;

   @BeforeClass
   public static void deployJar() {
      undeployJar();

      JavaArchive archive = ShrinkWrap.create(JavaArchive.class, ENTITIES_JAR).addClass(ClassRegistryIT.class).addClass(TestEntity.class);
      String serverDir = System.getProperty("server1.dist");
      File deployment = new File(serverDir, "/standalone/deployments/" + ENTITIES_JAR);
      archive.as(ZipExporter.class).exportTo(deployment, true);
      log.info("test-entities.jar copied to server deployment dir");
   }

   @AfterClass
   public static void undeployJar() {
      String serverDir = System.getProperty("server1.dist");

      File deployment = new File(serverDir, "/standalone/deployments/" + ENTITIES_JAR);
      if (deployment.exists()) {
         deployment.delete();
      }
      File deploymentMarker = new File(serverDir, "/standalone/deployments/" + ENTITIES_JAR + ".deployed");
      if (deploymentMarker.exists()) {
         deploymentMarker.delete();
      }
   }

   @Indexed
   public static class TestEntity {

      @Field(analyze = Analyze.NO, store = Store.YES)
      public String name;

      public TestEntity(String name) {
         this.name = name;
      }
   }

   @Test
   @WithRunningServer(@RunningServer(name = "standalone-classregistry"))
   public void testIfDeployedCacheContainsProperValues() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);
      RemoteCache<Object, Object> remoteCache = rcm.getCache();

      remoteCache.put(3, "word1");
      assertEquals(1, remoteCache.size());
   }
}
