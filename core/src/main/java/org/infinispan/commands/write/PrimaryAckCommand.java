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
 * @author Radim Vansa
 * @since 9.0
 */
public class PrimaryAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 60;
   private CommandInvocationId commandInvocationId;
   private Object returnValue;
   private Throwable exception;
   private boolean successful;
   private SequentialInterceptorChain chain;

   public PrimaryAckCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      NonTxDistributionInterceptor interceptor = chain.findInterceptorExtending(NonTxDistributionInterceptor.class);
      if (interceptor == null) {
         return null;
      }
      interceptor.primaryAck(commandInvocationId, getOrigin(), returnValue, exception, successful);
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
      output.writeObject(returnValue);
      output.writeObject(exception);
      output.writeBoolean(successful);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = (CommandInvocationId) input.readObject();
      returnValue = input.readObject();
      exception = (Throwable) input.readObject();
      successful = input.readBoolean();
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

   public void setResult(Object returnValue, Throwable exception, boolean successful) {
      this.returnValue = returnValue;
      this.exception = exception;
      this.successful = successful;
   }
}
