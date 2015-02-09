package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Set;

import org.infinispan.commands.EntryProcessor;
import org.infinispan.commands.Visitor;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * Generalized write command.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryProcessCommand extends AbstractDataWriteCommand {
   public static final byte COMMAND_ID = 48;

   EntryProcessor processor;
   boolean retry;
   CacheNotifier notifier;
   boolean successful = true;

   public EntryProcessCommand() {
   }

   public EntryProcessCommand(Object key, EntryProcessor processor, boolean retry,
                             CacheNotifier notifier, Set<Flag> flags) {
      super(key, flags);
      this.processor = processor;
      this.retry = retry;
      this.notifier = notifier;
   }

   public void init(CacheNotifier notifier, Configuration cfg) {
      this.notifier = notifier;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEntryProcessCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e == null) {
         successful = false;
         // TODO: isolate the cases when this can happen
         throw new IllegalStateException("Entry not wrapped!");
      }

      MutableEntryImpl mutableEntry = new MutableEntryImpl(e.getKey(), e.getValue(), e.getMetadata());
      Object result = processor.process(mutableEntry, retry);
      if (result == null) {
         throw new NullPointerException(
               String.format("Entry processor has returned null: processor=%s, key=%s, value=%s, metadata=%s",
                     processor, key, e.getValue(), e.getMetadata(), retry));
      }

      if (mutableEntry.getValue() == null && e.getValue() != null) {
         if (mutableEntry.isMetadataSet()) {
            throw new IllegalArgumentException("Value is null but metadata is " + mutableEntry.getMetadata());
         }
         notifier.notifyCacheEntryRemoved(key, e.getValue(), e.getMetadata(), true, ctx, this);
         e.setValue(null);
         e.setRemoved(true);
         e.setValid(false);
         e.setChanged(true);
         e.setCreated(false);
         successful = true;
      } else if (mutableEntry.getValue() != null) {
         e.setValue(mutableEntry.getValue());
         if (mutableEntry.isMetadataSet())
         e.setMetadata(mutableEntry.getMetadata());
         if (e.isRemoved()) {
            e.setCreated(true);
         }
         if (e.getValue() == null) {
            notifier.notifyCacheEntryCreated(key, mutableEntry.getValue(), true, ctx, this);
            e.setCreated(true);
         } else {
            notifier.notifyCacheEntryModified(key, mutableEntry.getValue(), e.getValue(), e.getMetadata(), false, ctx, this);
         }
         e.setRemoved(false);
         e.setValid(true);
         e.setChanged(true);
         successful = true;
      }

      return result;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, processor, retry, Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      processor = (EntryProcessor) parameters[1];
      retry = (boolean) parameters[2];
      flags = (Set<Flag>) parameters[3];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      EntryProcessCommand that = (EntryProcessCommand) o;

      if (processor.equals(that.processor)) return false;
      if (retry != that.retry) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + processor.hashCode();
      result = 31 * result + (retry ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("EntryProcessCommand{key=")
            .append(toStr(key))
            .append(", flags=").append(flags)
            .append(", processor=").append(processor)
            .append(", retry=").append(retry)
            .append(", successful=").append(successful)
            .append("}")
            .toString();
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // noop
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
   }

   public EntryProcessor getProcessor() {
      return processor;
   }

   public void setRetry(boolean retry) {
      this.retry = retry;
   }
}
