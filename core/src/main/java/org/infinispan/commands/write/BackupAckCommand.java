package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class BackupAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 59;
   private CommandInvocationId commandInvocationId;
   private SequentialInterceptorChain chain;

   public BackupAckCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      NonTxDistributionInterceptor interceptor = chain.findInterceptorExtending(NonTxDistributionInterceptor.class);
      if (interceptor == null) {
         return null;
      }
      interceptor.ack(commandInvocationId, getOrigin());
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = (CommandInvocationId) input.readObject();
   }

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   public void setCommandInvocationId(CommandInvocationId commandInvocationId) {
      this.commandInvocationId = commandInvocationId;
   }

   public void setInterceptorChain(SequentialInterceptorChain chain) {
      this.chain = chain;
   }
}
