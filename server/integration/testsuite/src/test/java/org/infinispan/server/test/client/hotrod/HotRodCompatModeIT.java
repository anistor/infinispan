package org.infinispan.server.test.client.hotrod;

import java.io.IOException;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.infinispan.server.test.query.TestEntity;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test remote cache with compat mode and custom marshaller based on JBoss marshalling.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@RunWith(Arquillian.class)
@Category(HotRodSingleNode.class)
public class HotRodCompatModeIT {

   private static final String CACHE_NAME = "compatibilityCache2";

   private static RemoteCacheManager remoteCacheManager;

   private RemoteCache<Object, Object> remoteCache;

   private static final int CACHE_SIZE = 1000;

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @Deployment(testable = false, name = "myTestDeployment")
   @TargetsContainer("container1")
   public static Archive<?> deploy() throws IOException {
      return createArchive();
   }

   private static Archive<?> createArchive() throws IOException {
      return ShrinkWrap.create(JavaArchive.class, "hotrod-compat-test.jar")
            // Add custom marshaller classes
            //.addClasses(CustomProtoStreamMarshaller.class, ProtoStreamMarshaller.class, BaseProtoStreamMarshaller.class)
            .addClasses(TestEntity.class, HotRodTestMarshaller.class)
            // Add marshaller dependencies
            //.add(new StringAsset(protoFile), "/sample_bank_account/bank.proto")
            //.add(new StringAsset("Dependencies: org.infinispan.protostream"), "META-INF/MANIFEST.MF")
            // Register marshaller
            //.addAsServiceProvider(Marshaller.class, CustomProtoStreamMarshaller.class)
            // Add custom filterConverter classes
            //.addClasses(CustomFilterFactory.class, CustomFilterFactory.CustomFilter.class, ParamCustomFilterFactory.class, ParamCustomFilterFactory.ParamCustomFilter.class)
            // Register custom filterConverterFactories
            //.addAsServiceProviderAndClasses(KeyValueFilterConverterFactory.class, ParamCustomFilterFactory.class, CustomFilterFactory.class)
            ;
   }

   @Before
   public void setup() throws IOException {
      RemoteCacheManagerFactory remoteCacheManagerFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort()).marshaller(new HotRodTestMarshaller());
      remoteCacheManager = remoteCacheManagerFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
   }

   @AfterClass
   public static void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   @Test
   public void testIterationWithComplexValues() {
//      remoteCache.clear();
//      IntStream.range(0, CACHE_SIZE).forEach(k -> remoteCache.put(k, new TestEntity("value" + k)));
//      Set<Object> keys = new HashSet<>();
//      try (CloseableIterator<Map.Entry<Object, Object>> iter = remoteCache.retrieveEntries(null, 10)) {
//         iter.forEachRemaining(e -> keys.add(e.getKey()));
//      }
//      assertEquals(CACHE_SIZE, keys.size());
   }
}


