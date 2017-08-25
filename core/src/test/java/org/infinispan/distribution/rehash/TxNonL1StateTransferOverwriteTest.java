package org.infinispan.distribution.rehash;

import org.testng.annotations.Test;

/**
 * Test that ensures that state transfer values aren't overridden with a tx cache without L1 enabled.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.TxNonL1StateTransferOverwriteTest")
public class TxNonL1StateTransferOverwriteTest extends BaseTxStateTransferOverwriteTest {
   public TxNonL1StateTransferOverwriteTest() {
      l1CacheEnabled = false;
   }

   public void testNonCoordinatorOwnerLeavingDuringReplace() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REPLACE);
   }

   public void testNonCoordinatorOwnerLeavingDuringReplaceFunctional() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REPLACE);
   }

   public void testNonCoordinatorOwnerLeavingDuringReplaceExact() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REPLACE_EXACT);
   }

   public void testNonCoordinatorOwnerLeavingDuringReplaceExactFunctional() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REPLACE_EXACT_FUNCTIONAL);
   }

   public void testNonCoordinatorOwnerLeavingDuringRemove() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REMOVE);
   }

   public void testNonCoordinatorOwnerLeavingDuringRemoveFunctional() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REMOVE_FUNCTIONAL);
   }

   public void testNonCoordinatorOwnerLeavingDuringRemoveExact() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REMOVE_EXACT);
   }

   public void testNonCoordinatorOwnerLeavingDuringRemoveExactFunctional() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REMOVE_EXACT_FUNCTIONAL);
   }

   public void testNonCoordinatorOwnerLeavingDuringPutOverwrite() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.PUT_OVERWRITE);
   }

   public void testNonCoordinatorOwnerLeavingDuringPutOverwriteFunctional() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.PUT_OVERWRITE_FUNCTIONAL);
   }
}
