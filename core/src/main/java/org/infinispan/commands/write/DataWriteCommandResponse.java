package org.infinispan.commands.write;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.util.concurrent.CompletableFutures;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class DataWriteCommandResponse {

   private final Object returnValue;
   private final boolean successful;
   private final CompletableFuture<?> backups;
   private final CompletableFuture<Void> entriesUpdated;

   public DataWriteCommandResponse(Object returnValue, CompletableFuture<?> backups) {
      this(returnValue, backups, true);
   }

   public DataWriteCommandResponse(Object returnValue) {
      this(returnValue, CompletableFutures.COMPLETED_FUTURE, false);
   }

   public DataWriteCommandResponse(Object returnValue, boolean successfull) {
      this(returnValue, CompletableFutures.COMPLETED_FUTURE, CompletableFutures.COMPLETED_FUTURE, successfull);
   }

   private DataWriteCommandResponse(Object returnValue, CompletableFuture<?> backups, boolean successful) {
      this(returnValue, backups, new CompletableFuture<>(), successful);
   }

   private DataWriteCommandResponse(Object returnValue, CompletableFuture<?> backups,
                                    CompletableFuture<Void> entriesUpdated, boolean successful) {
      this.returnValue = returnValue;
      this.backups = backups;
      this.successful = successful;
      this.entriesUpdated = entriesUpdated;
   }

   public Object getReturnValue() {
      return returnValue;
   }

   public CompletableFuture<?> getBackups() {
      return backups;
   }

   public CompletableFuture<Void> getEntriesUpdated() {
      return entriesUpdated;
   }

   public void notifyEntriesUpdated() {
      entriesUpdated.complete(null);
   }

   public boolean isSuccessful() {
      return successful;
   }

   public static class Externalizer implements AdvancedExternalizer<DataWriteCommandResponse> {

      @Override
      public Set<Class<? extends DataWriteCommandResponse>> getTypeClasses() {
         return Collections.singleton(DataWriteCommandResponse.class);
      }

      @Override
      public Integer getId() {
         return Ids.DATA_WRITE_RESPONSE;
      }

      @Override
      public void writeObject(ObjectOutput output, DataWriteCommandResponse object) throws IOException {
         output.writeObject(object.returnValue);
         output.writeBoolean(object.successful);
      }

      @Override
      public DataWriteCommandResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new DataWriteCommandResponse(input.readObject(), CompletableFutures.COMPLETED_FUTURE,
                                             CompletableFutures.COMPLETED_FUTURE, input.readBoolean());
      }
   }
}
