package org.infinispan.server.infinispan.query;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
public class ClassRegistryService implements Service<ClassRegistry> {

   public static final ServiceName SERVICE_NAME = ServiceName.JBOSS.append("ClassRegistry");

   private final ClassRegistry registry = new ClassRegistryImpl();

   @Override
   public ClassRegistry getValue() {
      return registry;
   }

   @Override
   public void start(StartContext context) throws StartException {
      InfinispanLogger.ROOT_LOGGER.debugf("Starting ClassRegistryService");
   }

   @Override
   public void stop(StopContext context) {
      InfinispanLogger.ROOT_LOGGER.debugf("Stopping ClassRegistryService");
   }
}
