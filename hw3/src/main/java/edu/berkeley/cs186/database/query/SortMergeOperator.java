package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.sun.org.apache.regexp.internal.RE;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }


  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator extends JoinIterator {
    //add any member variables you need here
    String leftTableName;
    String rightTableName;
    private BacktrackingIterator<Record> leftRecordIterator;
    private BacktrackingIterator<Record> rightRecordIterator;
    private Record leftRecord;
    private Record rightRecord;
    private Boolean wasEqual;
    private Record nextRecord;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      String leftTableName = getLeftTableName();
      String rightTableName = getRightTableName();

      SortOperator leftSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), leftTableName, new LeftRecordComparator());
      SortOperator rightSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), rightTableName, new RightRecordComparator());
      this.leftTableName = leftSortOperator.sort();
      this.rightTableName = rightSortOperator.sort();

      try {
        this.leftRecordIterator = SortMergeOperator.this.getRecordIterator(this.leftTableName);
        this.leftRecord = this.leftRecordIterator.next();
        this.leftRecordIterator.mark();
      } catch (DatabaseException | NoSuchElementException e) {
        this.leftRecordIterator = null;
        this.leftRecord = null;
      }

      try {
        this.rightRecordIterator = SortMergeOperator.this.getRecordIterator(this.rightTableName);
        this.rightRecord = this.rightRecordIterator.next();
        this.rightRecordIterator.mark();
      } catch (DatabaseException | NoSuchElementException e) {
        this.rightRecordIterator = null;
        this.rightRecord = null;
      }
      this.wasEqual = false;
    }

    private int compareRecords(Record leftRecord, Record rightRecord) {
      DataBox leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
      DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
      if (leftJoinValue.equals(rightJoinValue)) {
        List<DataBox> leftValues = new ArrayList<DataBox>(leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
        return 0;
      } else {
        return leftJoinValue.compareTo(rightJoinValue);
      }
    }

    private void advanceLeftRecord() {
      try {
        this.leftRecord = this.leftRecordIterator.next();
      } catch (NoSuchElementException e) {
        this.leftRecord = null;
      }
    }

    private void advanceRightRecord() {
      try {
        this.rightRecord = this.rightRecordIterator.next();
      } catch (NoSuchElementException e) {
        this.rightRecord = null;
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      } else {
        while (true) {
          if (this.leftRecord == null) {
            return false;
          } else if (this.rightRecord == null) {
            if (this.wasEqual) {
              this.rightRecordIterator.reset();
              advanceRightRecord();
              advanceLeftRecord();
              this.wasEqual = false;
            } else {
              return false;
            }
          } else {
            if (compareRecords(this.leftRecord, this.rightRecord) != 0) {
              if (this.wasEqual) {
                this.rightRecordIterator.reset();
                advanceRightRecord();
                advanceLeftRecord();
                this.wasEqual = false;
              } else {
                while (compareRecords(this.leftRecord, this.rightRecord) > 0) {
                  this.wasEqual = false;
                  advanceRightRecord();
                }

                while (compareRecords(this.leftRecord, this.rightRecord) < 0) {
                  this.wasEqual = false;
                  advanceLeftRecord();
                }
              }
            } else if (compareRecords(this.leftRecord, this.rightRecord) == 0) {
              if (this.wasEqual == false) {
                this.rightRecordIterator.mark();
              }
              this.wasEqual = true;
              advanceRightRecord();
              return true;
            }
          }
        }
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (hasNext()) {
        Record ret = this.nextRecord;
        this.nextRecord = null;
        return ret;
      } else {
        throw new NoSuchElementException();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
