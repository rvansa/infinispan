/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.tx;


import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "tx.Use1PcForInducedTxLockingTest")
public class Use1PcForInducedTxLockingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().use1PcForAutoCommitTransactions(true);
      dcc.clustering().hash().numOwners(1);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testCorrectLocking() {
      Object k0 = getKeyForCache(0);
      cache(1).put(k0, "v0");
      assertNotLocked(k0);
      assertNoTransactions();
      cache(0).put(k0, "v0");
      assertNotLocked(k0);
      assertNoTransactions();
   }
}
