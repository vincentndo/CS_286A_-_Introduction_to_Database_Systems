package edu.berkeley.cs186.database.query;

import com.sun.org.apache.regexp.internal.RE;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import java.util.*;
import java.util.prefs.BackingStoreException;


public class SortOperator  {
  private Database.Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Database.Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }


  public class Run {
    private String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      for (Record r: records) {
        this.addRecord(r.getValues());
      }
    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }


  /**
   * Returns a NEW run that is the sorted version of the input run.
   * Can do an in memory sort over all the records in this run
   * using one of Java's built-in sorting methods.
   * Note: Don't worry about modifying the original run.
   * Returning a new run would bring one extra page in memory beyond the
   * size of the buffer, but it is done this way for ease.
   */
  public Run sortRun(Run run) throws DatabaseException {
    Run ret = createRun();
    List<Record> recordList = new ArrayList<>();
    Iterator<Record> runIterator = run.iterator();
    while (runIterator.hasNext()) {
      recordList.add(runIterator.next());
    }
    Collections.sort(recordList, this.comparator);
    ret.addRecords(recordList);
    return ret;
  }



  /**
   * Given a list of sorted runs, returns a new run that is the result
   * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
   * to determine which record should be should be added to the output run next.
   * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
   * where a Pair (r, i) is the Record r with the smallest value you are
   * sorting on currently unmerged from run i.
   */
  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
    Run ret = createRun();
    RecordPairComparator pairComparator = new RecordPairComparator();
    PriorityQueue< Pair<Record, Integer> > recordPQueue = new PriorityQueue<>(pairComparator);
    List<Record> recordList = new ArrayList<>();
    List< Iterator<Record> > iteratorList = new ArrayList<>();

    for (int i = 0; i < runs.size(); i++) {
      Iterator<Record> recordIterator = runs.get(i).iterator();
      iteratorList.add(recordIterator);
      if (recordIterator.hasNext()) {
        recordPQueue.add(new Pair<>(recordIterator.next(), i));
      }
    }

    while (recordPQueue.isEmpty() == false) {
      Pair<Record, Integer> pair = recordPQueue.poll();
      int i = pair.getSecond();
      recordList.add(pair.getFirst());
      if (iteratorList.get(i).hasNext()) {
        recordPQueue.add(new Pair<>(iteratorList.get(i).next(), i));
      }
    }

    ret.addRecords(recordList);
    return ret;
  }

  /**
   * Given a list of N sorted runs, returns a list of
   * sorted runs that is the result of merging (numBuffers - 1)
   * of the input runs at a time.
   */
  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
    List<Run> ret = new ArrayList<>();

    while (runs.isEmpty() == false) {
      List<Run> tempRunList = new ArrayList<>();
      int n = java.lang.Math.min(this.numBuffers - 1, runs.size());
      for (int i = 0; i < n; i++) {
        tempRunList.add(runs.remove(0));
      }
      ret.add(mergeSortedRuns(tempRunList));
    }
    return ret;
  }


  /**
   * Does an external merge sort on the table with name tableName
   * using numBuffers.
   * Returns the name of the table that backs the final run.
   */
  public String sort() throws DatabaseException {
    List<Run> allRunList = new ArrayList<>();

    Iterator<Page> pageIterator = this.transaction.getPageIterator(this.tableName);
    pageIterator.next();

    while (pageIterator.hasNext()) {
      List<Run> runList = new ArrayList<>();
      for (int i = 0; i < this.numBuffers; i++) {
        try {
          Page[] pageList = {pageIterator.next()};
          Iterator<Record> blockIterator = this.transaction.getBlockIterator(this.tableName, pageList);
          Run run = createRun();
          while(blockIterator.hasNext()) {
            run.addRecord(blockIterator.next().getValues());
          }
          runList.add(run);
        } catch (NoSuchElementException e) {
          break;
        }
      }

      for (int i = 0; i < runList.size(); i++) {
        Run newRun = this.sortRun(runList.get(i));
        allRunList.add(newRun);
      }
    }

    while (allRunList.size() > 1) {
      allRunList = this.mergePass(allRunList);
    }

    return allRunList.get(0).tableName();
  }


  private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
    public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
      return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

    }
  }

  public Run createRun() throws DatabaseException {
    return new Run();
  }
}
