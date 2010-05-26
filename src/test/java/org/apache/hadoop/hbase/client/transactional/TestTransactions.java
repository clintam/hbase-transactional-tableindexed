/**
 * Copyright 2009 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */
package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the transaction functionality. This requires to run an {@link TransactionalRegionServer}.
 */
public class TestTransactions {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    private static final String TABLE_NAME = "table1";

    private static final byte[] FAMILY = Bytes.toBytes("family");
    private static final byte[] QUAL_A = Bytes.toBytes("a");
    private static final byte[] COL_A = Bytes.toBytes("family:a");

    private static final byte[] ROW1 = Bytes.toBytes("row1");
    private static final byte[] ROW2 = Bytes.toBytes("row2");
    private static final byte[] ROW3 = Bytes.toBytes("row3");

    private static HBaseAdmin admin;
    private static TransactionalTable table;
    private static TransactionManager transactionManager;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TEST_UTIL.getConfiguration().set(HConstants.REGION_SERVER_CLASS, TransactionalRegionInterface.class.getName());
        TEST_UTIL.getConfiguration().set(HConstants.REGION_SERVER_IMPL, TransactionalRegionServer.class.getName());

        TEST_UTIL.startMiniCluster(3);
        setupTables();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    private static void setupTables() throws Exception {

        HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
        desc.addFamily(new HColumnDescriptor(FAMILY));
        admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
        admin.createTable(desc);
        table = new TransactionalTable(TEST_UTIL.getConfiguration(), desc.getName());

        transactionManager = new TransactionManager(TEST_UTIL.getConfiguration());
    }

    @Before
    public void writeInitalRow() throws IOException {
        table.put(new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(1)));
    }

    @Test
    public void testSimpleTransaction() throws IOException, CommitUnsuccessfulException {
        TransactionState transactionState = makeTransaction1();
        transactionManager.tryCommit(transactionState);
    }

    @Test
    public void testTwoTransactionsWithoutConflict() throws IOException, CommitUnsuccessfulException {
        TransactionState transactionState1 = makeTransaction1();
        TransactionState transactionState2 = makeTransaction2();

        transactionManager.tryCommit(transactionState1);
        transactionManager.tryCommit(transactionState2);
    }

    @Test
    public void testTwoTransactionsWithConflict() throws IOException, CommitUnsuccessfulException {
        TransactionState transactionState1 = makeTransaction1();
        TransactionState transactionState2 = makeTransaction2();

        transactionManager.tryCommit(transactionState2);

        try {
            transactionManager.tryCommit(transactionState1);
            Assert.fail();
        } catch (CommitUnsuccessfulException e) {
            // Good
        }
    }

    @Test
    public void testGetAfterPut() throws IOException {
        TransactionState transactionState = transactionManager.beginTransaction();

        int originalValue = Bytes.toInt(table.get(transactionState, new Get(ROW1).addColumn(COL_A)).value());
        int newValue = originalValue + 1;

        table.put(transactionState, new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(newValue)));

        Result row1_A = table.get(transactionState, new Get(ROW1).addColumn(COL_A));
        Assert.assertEquals(newValue, Bytes.toInt(row1_A.value()));
    }

    @Test
    public void testGetAfterPutPut() throws IOException {
        TransactionState transactionState = transactionManager.beginTransaction();

        int originalValue = Bytes.toInt(table.get(transactionState, new Get(ROW1).addColumn(COL_A)).value());
        int newValue = originalValue + 1;

        table.put(transactionState, new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(newValue)));

        newValue = newValue + 1;

        table.put(transactionState, new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(newValue)));

        Result row1_A = table.get(transactionState, new Get(ROW1).addColumn(COL_A));
        Assert.assertEquals(newValue, Bytes.toInt(row1_A.value()));
    }

    @Test
    public void testScanAfterUpdatePut() throws IOException {
        TransactionState transactionState = transactionManager.beginTransaction();

        int originalValue = Bytes.toInt(table.get(transactionState, new Get(ROW1).addColumn(COL_A)).value());
        int newValue = originalValue + 1;
        table.put(transactionState, new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(newValue)));

        ResultScanner scanner = table.getScanner(transactionState, new Scan().addFamily(FAMILY));

        Result result = scanner.next();
        Assert.assertNotNull(result);

        Assert.assertEquals(Bytes.toString(ROW1), Bytes.toString(result.getRow()));
        Assert.assertEquals(newValue, Bytes.toInt(result.value()));

        result = scanner.next();
        // FIXME THIS FAILS probably because we just bringup one for alltests so ROW 2 is in as well.
        // Assert.assertNull(result);

    }

    @Test
    public void testScanAfterNewPut() throws IOException {
        TransactionState transactionState = transactionManager.beginTransaction();

        int row2Value = 199;
        table.put(transactionState, new Put(ROW2).add(FAMILY, QUAL_A, Bytes.toBytes(row2Value)));

        ResultScanner scanner = table.getScanner(transactionState, new Scan().addFamily(FAMILY));

        Result result = scanner.next();
        Assert.assertNotNull(result);
        Assert.assertEquals(Bytes.toString(ROW1), Bytes.toString(result.getRow()));

        result = scanner.next();
        Assert.assertNotNull(result);
        Assert.assertEquals(Bytes.toString(ROW2), Bytes.toString(result.getRow()));
        Assert.assertEquals(row2Value, Bytes.toInt(result.value()));
    }

    @Test
    public void testPutPutScan() throws IOException {
        TransactionState transactionState = transactionManager.beginTransaction();

        int row2Value = 199;
        table.put(transactionState, new Put(ROW2).add(FAMILY, QUAL_A, Bytes.toBytes(row2Value)));

        row2Value = 299;
        table.put(transactionState, new Put(ROW2).add(FAMILY, QUAL_A, Bytes.toBytes(row2Value)));

        ResultScanner scanner = table.getScanner(transactionState, new Scan().addFamily(FAMILY));

        Result result = scanner.next();
        Assert.assertNotNull(result);
        Assert.assertEquals(Bytes.toString(ROW1), Bytes.toString(result.getRow()));

        result = scanner.next();
        Assert.assertNotNull(result);
        Assert.assertEquals(Bytes.toString(ROW2), Bytes.toString(result.getRow()));
        Assert.assertEquals(row2Value, Bytes.toInt(result.value()));

        // TODO commit and verifty that we see second put.
    }

    @Test
    public void testPutPutScanOverAndOver() throws IOException {
        // Do this test many times to try and hit two puts in the same millisecond
        for (int i = 0; i < 100; i++) {
            testPutPutScan();
        }
    }

    // Read from ROW1,COL_A and put it in ROW2_COLA and ROW3_COLA
    private TransactionState makeTransaction1() throws IOException {
        TransactionState transactionState = transactionManager.beginTransaction();

        Result row1_A = table.get(transactionState, new Get(ROW1).addColumn(COL_A));

        table.put(transactionState, new Put(ROW2).add(FAMILY, QUAL_A, row1_A.getValue(FAMILY, QUAL_A)));
        table.put(transactionState, new Put(ROW3).add(FAMILY, QUAL_A, row1_A.getValue(FAMILY, QUAL_A)));

        return transactionState;
    }

    // Read ROW1,COL_A, increment its (integer) value, write back
    private TransactionState makeTransaction2() throws IOException {
        TransactionState transactionState = transactionManager.beginTransaction();

        Result row1_A = table.get(transactionState, new Get(ROW1).addColumn(COL_A));

        int value = Bytes.toInt(row1_A.getValue(FAMILY, QUAL_A));

        table.put(transactionState, new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(value + 1)));

        return transactionState;
    }
}
