package org.infinispan.query.remote.impl.indexing;

import java.util.Base64;

import org.infinispan.query.Transformer;

/**
 * A key Transformer suitable for ProtobufValueWrapper. Uses base64 encoding to turn the inner byte array into a String
 * in a more efficient way than the {@link org.infinispan.query.impl.DefaultTransformer}.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public final class ProtobufValueWrapperTransformer implements Transformer {

   @Override
   public Object fromString(String str) {
      byte[] binary = Base64.getDecoder().decode(str);
      return new ProtobufValueWrapper(binary);
   }

   @Override
   public String toString(Object obj) {
      ProtobufValueWrapper pvw = (ProtobufValueWrapper) obj;
      return Base64.getEncoder().encodeToString(pvw.getBinary());
   }
}
