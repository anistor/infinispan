package org.infinispan.server.infinispan.query;

import java.util.List;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.annotation.CompositeIndex;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.DotName;
import org.jboss.modules.Module;
import org.jboss.msc.service.ServiceController;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class ClassRegistryProcessor implements DeploymentUnitProcessor {

   public static final String SERVICE_NAME = "ClassRegistry";

   private static final DotName INDEXED_ENTITY_ANNOTATION = DotName.createSimple("org.hibernate.search.annotations.Indexed");

   private static DotName[] INDEXED_ENTITIES_ANNOTATIONS = {INDEXED_ENTITY_ANNOTATION};

   @Override
   public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
      DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
      Module module = deploymentUnit.getAttachment(Attachments.MODULE);
      CompositeIndex index = deploymentUnit.getAttachment(Attachments.COMPOSITE_ANNOTATION_INDEX);
      if (module == null || index == null) {
         return;
      }

      ServiceController<?> serviceController = phaseContext.getServiceRegistry().getService(ClassRegistryService.SERVICE_NAME);
      ClassRegistry classRegistryService = (ClassRegistry) serviceController.getValue();

      for (DotName annotationName : INDEXED_ENTITIES_ANNOTATIONS) {
         final List<AnnotationInstance> annotationInstances = index.getAnnotations(annotationName);
         for (AnnotationInstance annotation : annotationInstances) {
            String className = annotation.target().asClass().toString();
            try {
               Class<?> c = module.getClassLoader().loadClass(className);
               classRegistryService.getClasses().put(className, c);
            } catch (ClassNotFoundException e) {
               throw new DeploymentUnitProcessingException(e);
            }
         }
      }
   }

   @Override
   public void undeploy(DeploymentUnit context) {
   }
}
