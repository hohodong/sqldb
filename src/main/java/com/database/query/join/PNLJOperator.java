package com.database.query.join;

import com.database.TransactionContext;
import com.database.query.QueryOperator;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Page Nested Loop Join algorithm.
 */
public class PNLJOperator extends BNLJOperator {
    public PNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction);

        joinType = JoinType.PNLJ;
        numBuffers = 3;
    }
}
