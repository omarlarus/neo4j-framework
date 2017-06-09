/*
 * Copyright (c) 2013-2017 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.tx.executor.input;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.PrefetchingIterator;

import com.graphaware.tx.executor.batch.BatchTransactionExecutor;
import com.graphaware.tx.executor.single.TransactionCallback;

/**
 * An {@link Iterable}, items of which are retrieved from the database in batches. Intended to be used as
 * input to implementations of {@link BatchTransactionExecutor}.
 *
 * @param <T> type of fetched input.
 */
public class TransactionalInput<T> extends PrefetchingIterator<T> implements Iterable<T>, Iterator<T> {

    private final GraphDatabaseService database;
    private final TransactionCallback<Iterable<T>> callback;
    private Iterator<T> iterator;
    private List<T> queryResultDump;

    /**
     * Construct a new input.
     *
     * @param database  from which to fetch input, must not be <code>null</code>.
     * @param batchSize size of batches in which input if fetched. Must be positive. (Deprecated)
     * @param callback  which actually retrieves an iterable from the database.
     */
    @Deprecated
    public TransactionalInput(GraphDatabaseService database, int batchSize, TransactionCallback<Iterable<T>> callback) {
    	// deprecated because from neo4j 3.2 the open/close transaction mechanism doesn't work
    	// now all the data are dumped in memory in order to take the transaction short
    	this(database, callback);
    }

    /**
     * Execute the callback in transaction scope
     * @param database
     * @param callback
     */
    public TransactionalInput(GraphDatabaseService database, TransactionCallback<Iterable<T>> callback) {
        Objects.requireNonNull(database);
        Objects.requireNonNull(callback);

        this.database = database;
        this.callback = callback;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    protected synchronized T fetchNextOrNull() {
    	
    	if(this.iterator == null){
    		this.iterator = fetchAll();
    	}

        return iterator.hasNext()? iterator.next(): null;
    }

	private Iterator<T> fetchAll() {
		try (Transaction tx = database.beginTx()){
			Iterator<T> it = callback.doInTransaction(database).iterator();
			queryResultDump = new ArrayList<>();
			while (it.hasNext()) {
				queryResultDump.add(it.next());
			}
			tx.success();
			return queryResultDump.iterator();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<T> iterator() {
        return this;
    }
}
