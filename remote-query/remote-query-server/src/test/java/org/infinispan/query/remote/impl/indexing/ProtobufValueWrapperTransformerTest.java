package org.infinispan.query.remote.impl.indexing;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 10.0
 */
@Test(groups = {"functional", "unit"}, testName = "query.remote.impl.indexing.ProtobufValueWrapperTransformerTest")
public class ProtobufValueWrapperTransformerTest {

   public void testTransformer() throws Exception {
      SerializationContext serCtx = ProtobufUtil.newSerializationContext();
      ProtobufValueWrapper pvw = new ProtobufValueWrapper(ProtobufUtil.toWrappedByteArray(serCtx, Boolean.TRUE));

      ProtobufValueWrapperTransformer pvwt = new ProtobufValueWrapperTransformer();
      String str = pvwt.toString(pvw);
      Object fromString = pvwt.fromString(str);
      assertEquals(ProtobufValueWrapper.class, fromString.getClass());
      assertEquals(pvw, fromString);
   }
}
