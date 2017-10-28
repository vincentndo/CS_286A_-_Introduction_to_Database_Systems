package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.BacktrackingIterator;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator extends JoinIterator {
    // add any member variables here
    String leftTableName;
    String rightTableName;
    private BacktrackingIterator<Page> leftIterator;
    private BacktrackingIterator<Page> rightIterator;
    private BacktrackingIterator<Record> leftPageIterator = null;
    private BacktrackingIterator<Record> rightPageIterator;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;

    private void getLeftRecord() {
      try {
        Page[] singlePage = {this.leftIterator.next()};
        this.leftPageIterator = PNLJOperator.this.getBlockIterator(this.leftTableName, singlePage);
      } catch (DatabaseException | NoSuchElementException e) {
        this.leftPageIterator = null;
      }

      if (this.leftPageIterator != null) {
        try {
          this.leftRecord = this.leftPageIterator.next();
          this.leftPageIterator.mark();
        } catch (NoSuchElementException e) {
          this.leftRecord = null;
        }

        try {
          this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
          this.rightIterator.next();    // Discard page 0 (header page)
          getRightRecord();
        } catch (DatabaseException e) {
          this.rightIterator = null;
        }
      }
    }

    private void getRightRecord() {
      try {
        Page[] singlePage = {this.rightIterator.next()};
        this.rightPageIterator = PNLJOperator.this.getBlockIterator(this.rightTableName, singlePage);
      } catch (DatabaseException | NoSuchElementException e) {
        this.rightPageIterator = null;
      }

      if (this.rightPageIterator != null) {
        try {
          this.rightRecord = this.rightPageIterator.next();
          this.rightPageIterator.mark();
          this.leftPageIterator.reset();
          this.leftRecord = this.leftPageIterator.next();
        } catch (NoSuchElementException e) {
          this.rightRecord = null;
        }
      }
    }

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      this.leftTableName = getLeftTableName();
      this.rightTableName = getRightTableName();

      try {
        this.leftIterator = PNLJOperator.this.getPageIterator(this.leftTableName);
        this.leftIterator.next();    // Discard page 0 (header page)
        getLeftRecord();
      } catch (DatabaseException | NoSuchElementException e) {
        this.leftIterator = null;
      }

      this.nextRecord = null;
    }

    private boolean checkMatch(Record leftRecord, Record rightRecord) {
      DataBox leftJoinValue = leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
      DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
      if (leftJoinValue.equals(rightJoinValue)) {
        List<DataBox> leftValues = new ArrayList<DataBox>(leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
        return true;
      } else {
        return false;
      }
    }

    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      } else {
        while (true) {
          if (this.leftPageIterator == null) {
            return false;
          } else if (this.rightPageIterator == null) {
            getLeftRecord();
          } else if (this.leftRecord == null) {
            getRightRecord();
          } else if (this.rightRecord == null) {
            if (this.leftRecord == null) {
              getRightRecord();
            } else {
              try {
                this.leftRecord = this.leftPageIterator.next();
                this.rightPageIterator.reset();
                this.rightRecord = this.rightPageIterator.next();
              } catch (NoSuchElementException e) {
                this.leftRecord = null;
              }
            }
          } else {
            if (checkMatch(this.leftRecord, rightRecord)) {
              try {
                this.rightRecord = this.rightPageIterator.next();
              } catch (NoSuchElementException e) {
                this.rightRecord = null;
              }
              return true;
            }

            try {
              this.rightRecord = this.rightPageIterator.next();
            } catch (NoSuchElementException e) {
              this.rightRecord = null;
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
  }
}
