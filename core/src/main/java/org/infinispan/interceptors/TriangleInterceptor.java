package org.infinispan.interceptors;

import org.infinispan.commands.write.DataWriteCommandResponse;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.responses.SuccessfulResponse;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class TriangleInterceptor extends CommandInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   private Object handleWriteCommands(InvocationContext ctx, WriteCommand command) throws Throwable {
      Object result = invokeNextInterceptor(ctx, command);
      if (result != null && result instanceof DataWriteCommandResponse) {
         return ctx.isOriginLocal() ?
               ((DataWriteCommandResponse) result).getReturnValue() :
               createResponse((DataWriteCommandResponse) result, command);
      }
      return result;
   }

   private static SuccessfulResponse createResponse(DataWriteCommandResponse response, WriteCommand command) {
      if (command.isReturnValueExpected()) {
         return SuccessfulResponse.create(response);
      } else {
         //don't need to send the return value.
         return SuccessfulResponse.create(new DataWriteCommandResponse(null, response.isSuccessful()));
      }
   }
}
