package com.database.query;

import com.database.TransactionContext;
import com.database.common.Pair;
import com.database.common.iterator.BacktrackingIterator;
import com.database.query.disk.Run;
import com.database.table.Record;
import com.database.table.Schema;
import com.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    protected Comparator<Pair<Record,Integer>> comparatorPair;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
        comparatorPair = new RecordIntPairComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    private class RecordIntPairComparator implements Comparator<Pair<Record,Integer>> {
        @Override
        public int compare(Pair<Record,Integer> r1, Pair<Record,Integer> r2) {
            return r1.getFirst().getValue(sortColumnIndex).compareTo(r2.getFirst().getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        List<Record> list = new ArrayList<>();
        records.forEachRemaining(list::add);
        list.sort(comparator);
        Run ret = new Run(transaction, getSchema());
        ret.addAll(list);
        return ret;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        Run ret = new Run(transaction, getSchema());
        PriorityQueue<Pair<Record, Integer>> pq = new PriorityQueue<>(comparatorPair);
        List<BacktrackingIterator<Record>> list = new ArrayList<>();
        for(int i=0;i< runs.size();i++){
            list.add(runs.get(i).iterator());
            if(list.get(i).hasNext()){
                pq.add(new Pair<Record, Integer>(list.get(i).next(), i));
            }

        }
        while(!pq.isEmpty()){
            Pair<Record, Integer> cur = pq.poll();
            ret.add(cur.getFirst());
            if(list.get(cur.getSecond()).hasNext()){
                pq.add(new Pair<Record, Integer>(list.get(cur.getSecond()).next(), cur.getSecond()));
            }
        }
        return ret;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        List<Run> ret = new ArrayList<>();
        int N = numBuffers-1;
        for(int i=0;i< runs.size();i+=N){
            if(i+N<= runs.size()){
                ret.add(mergeSortedRuns(runs.subList(i,i+N)));
            } else {
                ret.add(mergeSortedRuns(runs.subList(i, runs.size())));
            }

        }
        return ret;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

        List<Run> runs = new ArrayList<>();
        while (sourceIterator.hasNext()){
            runs.add(sortRun(getBlockIterator(sourceIterator, getSchema(), numBuffers)));
        }
        while(runs.size() != 1){
            runs = mergePass(runs);
        }
        return runs.iterator().next();
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

