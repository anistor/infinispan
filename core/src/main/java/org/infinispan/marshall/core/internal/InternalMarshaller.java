package org.infinispan.marshall.core.internal;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.GlobalComponentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

// TODO: If wrapped around GlobalMarshaller, there might not be need to implement StreamingMarshaller interface
// If exposed directly, e.g. not wrapped by GlobalMarshaller, it'd need to implement StreamingMarshaller
public final class InternalMarshaller implements StreamingMarshaller {

   final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();

   final Encoding<BytesObjectOutput, BytesObjectInput> enc = new BytesEncoding();
   final InternalExternalizerTable externalizers;

   // TODO: Default should be JBoss Marshaller (needs ExternalizerTable & GlobalConfiguration)
   // ^ External marshaller is what global configuration serialization config can tweak
   final StreamingMarshaller external = new JavaSerializationMarshaller();

   final GlobalComponentRegistry gcr;

   public InternalMarshaller(GlobalComponentRegistry gcr) {
      this.gcr = gcr;
      this.externalizers = new InternalExternalizerTable(enc, gcr);
   }

   @Override
   public void start() {
      externalizers.start();
   }

   @Override
   public void stop() {
      externalizers.stop();
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      int estimatedSize = getEstimatedSize(obj);
      BytesObjectOutput out = new BytesObjectOutput(estimatedSize, this);
      AdvancedExternalizer<Object> ext = externalizers.findWriteExternalizer(obj, out);
      if (ext != null) {
         ext.writeObject(out, obj);
         return out.toBytes();
      }

      // TODO: Delegate to external marshaller
      // TODO: Add finally section to update size estimates
      return null;
   }

   private int getEstimatedSize(Object obj) {
      return obj != null
            ? marshallableTypeHints.getBufferSizePredictor(obj.getClass()).nextSize(obj)
            : 1;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      ObjectInput in = new BytesObjectInput(buf, this);
      AdvancedExternalizer<Object> ext = externalizers.findReadExternalizer(in);
      if (ext != null)
         return ext.readObject(in);
      else {
         try {
            return external.objectFromObjectStream(in);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
         }
      }
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      BytesObjectOutput out = new BytesObjectOutput(estimatedSize, this);
      return new StreamBytesObjectOutput(os, out);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      out.writeObject(obj);
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      try {
         oo.flush();
      } catch (IOException e) {
         // ignored
      }
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf); // ignore offset and lengths
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      // TODO: Customise this generated block
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return new byte[0];  // TODO: Customise this generated block
   }

}
