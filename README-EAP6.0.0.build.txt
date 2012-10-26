This branch is to build the infinispan version for EAP 6.0.0.

To build the EAP 6.0.0 version use

  mvn clean install -s support-eap-6.0.0-settings.xml -Dmaven.repo.local=local-repo -Dmaven.test.skip=true


The includes patches are:
http://download.lab.bos.redhat.com/brewroot/repos/jb-eap-6.0.0-ga-rhel-6-updates-build/latest/maven/org/infinispan/infinispan/5.1.4.FINAL-redhat-1/org.infinispan-infinispan-5.1.4.FINAL-redhat-1-patches.zip

 infinispan-removeClasses.patch
 infinispan-vman-pom.patch

see: https://docspace.corp.redhat.com/docs/DOC-120905

