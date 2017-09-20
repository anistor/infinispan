package org.infinispan.configuration.module;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

/**
 * MyParserExtension. This is a simple extension parser which parses modules in the "urn:infinispan:config:mymodule" namespace
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Namespace(uri = "urn:infinispan:config:mymodule", root = "sample-element")
@Namespace(root = "sample-element")
@MetaInfServices
public class MyParserExtension implements ConfigurationParser {

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      if (holder.getScope() != ParserScope.CACHE && holder.getScope() != ParserScope.CACHE_TEMPLATE) {
         throw new IllegalStateException("WRONG SCOPE");
      }
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case SAMPLE_ELEMENT: {
         parseSampleElement(reader, builder.addModule(MyModuleConfigurationBuilder.class));
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseSampleElement(XMLExtendedStreamReader reader, MyModuleConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case SAMPLE_ATTRIBUTE: {
            builder.attribute(value);
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
