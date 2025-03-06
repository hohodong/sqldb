package com.database.query;

import com.database.Database;
import com.database.TestUtils;
import com.database.TimeoutScaling;
import com.database.Transaction;
import com.database.categories.Proj3Part2Tests;
import com.database.categories.Proj3Tests;
import com.database.categories.PublicTests;
import com.database.common.PredicateOperator;
import com.database.table.Record;
import com.database.table.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertTrue;

@Category({Proj3Tests.class, Proj3Part2Tests.class})
public class TestSingleAccess {
    private Database db;

    //Before every test you create a temp folder, after every test you close it
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 2 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                20000 * TimeoutScaling.factor)));

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder("testSingleAccess");
        String filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(5); // B=5

        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
            Schema schema = TestUtils.createSchemaWithAllTypes();

            // table without any indices
            t.createTable(schema, "table");

            // table with an index on `int`
            t.createTable(schema, "indexed_table");
            t.createIndex("indexed_table", "int", false);

            // table with an index on `int` and `float`
            t.createTable(schema, "multi_indexed_table");
            t.createIndex("multi_indexed_table", "int", false);
            t.createIndex("multi_indexed_table", "float", false);
        }
        this.db.waitAllTransactions();
    }

    @After
    public void afterEach() {
        this.db.waitAllTransactions();
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
    }

    @Test
    @Category(PublicTests.class)
    public void testSequentialScanSelection() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table", r);
            }
            transaction.getTransactionContext().getTable("table").buildStatistics(10);
            // SELECT * FROM table AS t1;
            QueryPlan query = transaction.query("table", "t1");
            QueryOperator op = query.minCostSingleAccess("t1");

            // we should access table using a sequential scan
            assertTrue(op.isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleIndexScanSelection() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("indexed_table", r);
            }
            // SELECT * FROM indexed_table WHERE int = 9;
            transaction.getTransactionContext().getTable("indexed_table").buildStatistics(10);
            QueryPlan query = transaction.query("indexed_table");
            query.select("int", PredicateOperator.EQUALS, 9);
            QueryOperator op = query.minCostSingleAccess("indexed_table");

            // we should access using the index on `int`
            assertTrue(op.isIndexScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testPushDownSelects() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table", r);
            }
            transaction.getTransactionContext().getTable("table").buildStatistics(10);

            // SELECT * FROM table AS t1 WHERE int = 9;
            QueryPlan query = transaction.query("table", "t1");
            query.select("int", PredicateOperator.EQUALS, 9);
            QueryOperator op = query.minCostSingleAccess("t1");

            // the selection `int = 9` should have been pushed down on top of
            // the sequential scan.
            assertTrue(op.isSelect());
            assertTrue(op.getSource().isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testPushDownMultipleSelects() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table", r);
            }
            transaction.getTransactionContext().getTable("table").buildStatistics(10);

            // SELECT * FROM table WHERE int = 9 AND bool = false;
            QueryPlan query = transaction.query("table");
            query.select("int", PredicateOperator.EQUALS, 9);
            query.select("bool", PredicateOperator.EQUALS, false);
            QueryOperator op = query.minCostSingleAccess("table");

            // both selections (`int = 9` and `bool = false` should have been
            // pushed down.
            assertTrue(op.isSelect());
            assertTrue(op.getSource().isSelect());
            assertTrue(op.getSource().getSource().isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testNoValidIndices() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", (float) i);
                transaction.insert("multi_indexed_table", r);
            }
            transaction.getTransactionContext().getTable("multi_indexed_table").buildStatistics(10);

            // SELECT * FROM multi_indexed_table;
            QueryPlan query = transaction.query("multi_indexed_table");
            QueryOperator op = query.minCostSingleAccess("multi_indexed_table");

            // no selection predicates specified, just use a sequential scan
            assertTrue(op.isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testIndexSelectionAndPushDown() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", (float) i);
                transaction.insert("multi_indexed_table", r);
            }
            transaction.getTransactionContext().getTable("multi_indexed_table").buildStatistics(10);

            // SELECT * FROM multi_indexed_table WHERE int = 9 AND bool = false;
            QueryPlan query = transaction.query("multi_indexed_table");
            query.select("int", PredicateOperator.EQUALS, 9);
            query.select("bool", PredicateOperator.EQUALS, false);
            QueryOperator op = query.minCostSingleAccess("multi_indexed_table");

            // we can do an index scan on the column `int`, but we should still
            // push down the selection predicate `bool = false` afterwards.
            assertTrue(op.isSelect());
            assertTrue(op.getSource().isIndexScan());
        }
    }

    @Test
    public void testIndexTimeCost() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 100000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table", r);
            }
            transaction.getTransactionContext().getTable("table").buildStatistics(10);
            // SELECT * FROM table AS t1;
            QueryPlan query = transaction.query("table", "t1");
            QueryOperator op = query.minCostSingleAccess("t1");
            Iterator<Record> iterator = op.iterator();
            long startTime = System.currentTimeMillis();
            while(iterator.hasNext()){
                iterator.next();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("执行时间：" + (endTime - startTime) + " 毫秒");
        }
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 100000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("indexed_table", r);
            }
            // SELECT * FROM indexed_table WHERE int = 9;
            transaction.getTransactionContext().getTable("indexed_table").buildStatistics(10);
            QueryPlan query = transaction.query("indexed_table");
            query.select("int", PredicateOperator.EQUALS, 50000);
            QueryOperator op = query.minCostSingleAccess("indexed_table");
            Iterator<Record> iterator = op.iterator();
            long startTime = System.currentTimeMillis();
            while(iterator.hasNext()){
                iterator.next();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("执行时间：" + (endTime - startTime) + " 毫秒");
        }
    }


    @Test
    public void testIndexConcurrency() throws InterruptedException {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 1000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("indexed_table", r);
            }
            transaction.commit();
            // SELECT * FROM indexed_table WHERE int = 9;
            transaction.getTransactionContext().getTable("indexed_table").buildStatistics(10);
            System.out.println("数据插入完毕");
        }
        int threadNum = 40;
        List<Thread> threads = new ArrayList<>();
        for(int i=0;i<threadNum;i++){
            threads.add(new Thread(() -> {
                try(Transaction transaction = this.db.beginTransaction()) {
                    // SELECT * FROM indexed_table WHERE int = ?;
                    int queryValue = ThreadLocalRandom.current().nextInt(1000);
                    QueryPlan query = transaction.query("indexed_table");
                    query.select("int", PredicateOperator.EQUALS, queryValue);
                    Iterator<Record> iterator = query.execute();
                    while(iterator.hasNext()){
                        iterator.next();
                    }
                    System.out.println("执行完毕");
                    transaction.commit();
                }
            }));
        }
        long startTime = System.currentTimeMillis();
        for(int i=0;i<threadNum;i++){
            threads.get(i).start();
        }
        for(int i=0;i<threadNum;i++){
            threads.get(i).join();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("执行时间：" + (endTime - startTime) + " 毫秒");



    }

}
