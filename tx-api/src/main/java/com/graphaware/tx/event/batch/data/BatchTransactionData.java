/*
 * Copyright (c) 2013 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.tx.event.batch.data;

import org.apache.commons.collections4.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.*;

/**
 * Simulation of {@link org.neo4j.graphdb.event.TransactionData} for non-transactional (batch) environments.
 * It maintains information about changes performed on the database continuously and notifies the registered
 * {@link org.neo4j.graphdb.event.TransactionEventHandler}s (simulates a transaction commit) when the number of
 * mutations performed exceeds {@link #commitTxAfterMutations}.
 */
public class BatchTransactionData implements TransactionData {

    private final Set<Node> createdNodes = new HashSet<>();
    private final Map<IdAndKey, PropertyEntry<Node>> assignedNodeProperties = new HashMap<>();
    private final Map<IdAndKey, PropertyEntry<Node>> removedNodeProperties = new HashMap<>();
    private final Map<Node, Set<String>> assignedNodeLabels = new HashMap<>();
    private final Map<Node, Set<String>> removedNodeLabels = new HashMap<>();

    private final Set<Relationship> createdRelationships = new HashSet<>();
    private final Map<IdAndKey, PropertyEntry<Relationship>> assignedRelationshipProperties = new HashMap<>();
    private final Map<IdAndKey, PropertyEntry<Relationship>> removedRelationshipProperties = new HashMap<>();

    private final List<TransactionEventHandler> transactionEventHandlers = new LinkedList<>();
    private static final int COMMIT_TX_AFTER_MUTATIONS = 1000;
    private int numberOfMutations = 0;
    private int commitTxAfterMutations = COMMIT_TX_AFTER_MUTATIONS;
    private boolean commitInProgress = false;

    /**
     * Create a new instance of this transaction data with default {@link #commitTxAfterMutations} ({@link #COMMIT_TX_AFTER_MUTATIONS}.
     */
    public BatchTransactionData() {
    }

    /**
     * Create a new instance of this transaction data, providing the number of mutations before "commit" is simulated.
     *
     * @param commitTxAfterMutations number of mutations before commit is simulated.
     */
    public BatchTransactionData(int commitTxAfterMutations) {
        this.commitTxAfterMutations = commitTxAfterMutations;
    }

    /**
     * @see {@link org.neo4j.graphdb.GraphDatabaseService#registerTransactionEventHandler(org.neo4j.graphdb.event.TransactionEventHandler)}.
     */
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(TransactionEventHandler<T> handler) {
        clear(); //event handlers should be registered in the beginning, so no modules should depend on the cleared data

        if (!transactionEventHandlers.contains(handler)) {
            transactionEventHandlers.add(handler);
        }
        return handler;
    }

    /**
     * Simulate a transaction commit.
     */
    public void simulateCommit() {
        if (noChangesOccurred()) {
            clear();
            return;
        }

        commitInProgress = true;

        for (TransactionEventHandler handler : transactionEventHandlers) {
            try {
                Object result = handler.beforeCommit(this);
                handler.afterCommit(this, result);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        clear();

        commitInProgress = false;
    }

    private boolean noChangesOccurred() {
        return createdNodes.isEmpty()
                && assignedNodeProperties.isEmpty()
                && removedNodeProperties.isEmpty()
                && assignedNodeLabels.isEmpty()
                && removedNodeLabels.isEmpty()
                && createdRelationships.isEmpty()
                && assignedRelationshipProperties.isEmpty()
                && removedRelationshipProperties.isEmpty();
    }

    /**
     * Increment the number of mutations performed since the last "commit" and simulate a commit if needed.
     */
    private void incrementMutationsAndCommitIfNeeded() {
        if (++numberOfMutations > commitTxAfterMutations) {
            simulateCommit();
        }
    }

    /**
     * Clear all transaction data.
     */
    private void clear() {
        numberOfMutations = 0;
        createdNodes.clear();
        assignedNodeProperties.clear();
        removedNodeProperties.clear();
        assignedNodeLabels.clear();
        removedNodeLabels.clear();
        createdRelationships.clear();
        assignedRelationshipProperties.clear();
        removedRelationshipProperties.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Node> createdNodes() {
        return Collections.unmodifiableCollection(createdNodes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Node> deletedNodes() {
        return Collections.emptySet(); //batch mode - no deletions
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDeleted(Node node) {
        return false; //batch mode - no deletions
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<PropertyEntry<Node>> assignedNodeProperties() {
        return Collections.unmodifiableCollection(assignedNodeProperties.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<PropertyEntry<Node>> removedNodeProperties() {
        return Collections.unmodifiableCollection(removedNodeProperties.values());
    }

    /**
     * An API call that's missing in Neo4j (see https://github.com/neo4j/neo4j/issues/1065).
     *
     * @return all labels assigned to nodes in this transaction, keyed by {@link Node}.
     */
    public Map<Node, Set<String>> assignedNodeLabels() {
        return Collections.unmodifiableMap(assignedNodeLabels);
    }

    /**
     * An API call that's missing in Neo4j (see https://github.com/neo4j/neo4j/issues/1065).
     *
     * @return all labels removed from nodes in this transaction, keyed by {@link Node}.
     */
    public Map<Node, Set<String>> removedNodeLabels() {
        return Collections.unmodifiableMap(removedNodeLabels);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Relationship> createdRelationships() {
        return Collections.unmodifiableCollection(createdRelationships);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Relationship> deletedRelationships() {
        return Collections.emptySet();  //batch mode - no deletions
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDeleted(Relationship relationship) {
        return false; //batch mode - no deletions
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties() {
        return Collections.unmodifiableCollection(assignedRelationshipProperties.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<PropertyEntry<Relationship>> removedRelationshipProperties() {
        return Collections.unmodifiableCollection(removedRelationshipProperties.values());
    }

    /**
     * Inform this object about a node that has been created.
     *
     * @param node that has been created.
     */
    public void nodeCreated(Node node) {
        if (commitInProgress) {
            return;
        }

        createdNodes.add(node);

        for (String key : node.getPropertyKeys()) {
            assignedNodeProperties.put(new IdAndKey(node.getId(), key),
                    PropertyEntryImpl.assigned(node, key, node.getProperty(key), null));
        }

        incrementMutationsAndCommitIfNeeded();
    }

    /**
     * Inform this object about a relationship that has been created.
     *
     * @param relationship that has been created.
     */
    public void relationshipCreated(Relationship relationship) {
        if (commitInProgress) {
            return;
        }

        createdRelationships.add(relationship);

        for (String key : relationship.getPropertyKeys()) {
            assignedRelationshipProperties.put(new IdAndKey(relationship.getId(), key),
                    PropertyEntryImpl.assigned(relationship, key, relationship.getProperty(key), null));
        }

        incrementMutationsAndCommitIfNeeded();
    }

    /**
     * Inform this object about a new value of a node's property about to be set. After it has been done,
     * {@link #nodePropertySet(org.neo4j.graphdb.Node, String, Object)} must be called.
     *
     * @param node  on which a new property value is about to be set.
     * @param key   of the property.
     * @param value of the property about to be set.
     */
    public void nodePropertyToBeSet(Node node, String key, Object value) {
        if (commitInProgress) {
            return;
        }

        IdAndKey idAndKey = new IdAndKey(node.getId(), key);

        Object previousValue;
        if (removedNodeProperties.containsKey(idAndKey)) {
            previousValue = removedNodeProperties.get(idAndKey).previouslyCommitedValue();
            removedNodeProperties.remove(idAndKey);
        } else if (assignedNodeProperties.containsKey(idAndKey)) {
            previousValue = assignedNodeProperties.get(idAndKey).previouslyCommitedValue();
        } else {
            previousValue = node.getProperty(key, null);
        }

        assignedNodeProperties.put(idAndKey, PropertyEntryImpl.assigned(node, key, value, previousValue));
    }

    /**
     * Inform this object about a new value of a node's property that has been set. Before this is called,
     * {@link #nodePropertyToBeSet(org.neo4j.graphdb.Node, String, Object)} must be called.
     *
     * @param node  on which a new property value has been set.
     * @param key   of the property.
     * @param value of the property set.
     */
    public void nodePropertySet(Node node, String key, Object value) {
        if (commitInProgress) {
            return;
        }

        incrementMutationsAndCommitIfNeeded();
    }

    /**
     * Inform this object about a new set of labels about to be assigned to a node. After it has been done,
     * {@link #nodeLabelsSet(org.neo4j.graphdb.Node, org.neo4j.graphdb.Label...)} must be called.
     *
     * @param node   to which a new set of labels is about to be assigned.
     * @param labels new labels.
     */
    public void nodeLabelsToBeSet(Node node, Label... labels) {
        if (commitInProgress) {
            return;
        }

        Set<String> newLabels = new HashSet<>();
        for (Label label : labels) {
            newLabels.add(label.name());
        }

        Set<String> existingLabels = new HashSet<>();
        for (Label label : node.getLabels()) {
            existingLabels.add(label.name());
        }

        Collection<String> removedLabels = CollectionUtils.subtract(existingLabels, newLabels);
        Collection<String> assignedLabels = CollectionUtils.subtract(newLabels, existingLabels);

        for (String removedLabel : removedLabels) {
            if (assignedNodeLabels.containsKey(node)) {
                assignedNodeLabels.get(node).remove(removedLabel);
            }

            if (!removedNodeLabels.containsKey(node)) {
                removedNodeLabels.put(node, new HashSet<String>());
            }

            removedNodeLabels.get(node).add(removedLabel);
        }

        for (String assignedLabel : assignedLabels) {
            if (removedNodeLabels.containsKey(node)) {
                removedNodeLabels.get(node).remove(assignedLabel);
            }

            if (!assignedNodeLabels.containsKey(node)) {
                assignedNodeLabels.put(node, new HashSet<String>());
            }

            assignedNodeLabels.get(node).add(assignedLabel);
        }
    }

    /**
     * Inform this object about a new set of labels assigned to a node. Before this is called,
     * {@link #nodeLabelsToBeSet(org.neo4j.graphdb.Node, org.neo4j.graphdb.Label...)} must be called.
     *
     * @param node   to which a new set of labels has been assigned.
     * @param labels new labels.
     */
    public void nodeLabelsSet(Node node, Label... labels) {
        if (commitInProgress) {
            return;
        }

        incrementMutationsAndCommitIfNeeded();
    }

    /**
     * Inform this object about a new value of a relationship's property about to be set. After it has been done,
     * {@link #relationshipPropertySet(org.neo4j.graphdb.Relationship, String, Object)}  must be called.
     *
     * @param relationship on which a new property value is about to be set.
     * @param key          of the property.
     * @param value        of the property about to be set.
     */
    public void relationshipPropertyToBeSet(Relationship relationship, String key, Object value) {
        if (commitInProgress) {
            return;
        }

        IdAndKey idAndKey = new IdAndKey(relationship.getId(), key);

        Object previousValue;
        if (removedRelationshipProperties.containsKey(idAndKey)) {
            previousValue = removedRelationshipProperties.get(idAndKey).previouslyCommitedValue();
            removedRelationshipProperties.remove(idAndKey);
        } else if (assignedRelationshipProperties.containsKey(idAndKey)) {
            previousValue = assignedRelationshipProperties.get(idAndKey).previouslyCommitedValue();
        } else {
            previousValue = relationship.getProperty(key, null);
        }

        assignedRelationshipProperties.put(idAndKey, PropertyEntryImpl.assigned(relationship, key, value, previousValue));
    }

    /**
     * Inform this object about a new value of a relationship's property that has been set. Before this is called,
     * {@link #relationshipPropertyToBeSet(org.neo4j.graphdb.Relationship, String, Object)}  must be called.
     *
     * @param relationship on which a new property value has been set.
     * @param key          of the property.
     * @param value        of the property set.
     */
    public void relationshipPropertySet(Relationship relationship, String key, Object value) {
        if (commitInProgress) {
            return;
        }

        incrementMutationsAndCommitIfNeeded();
    }

    /**
     * Inform this object about a node's property about to be removed. After is has been done,
     * {@link #nodePropertyRemoved(org.neo4j.graphdb.Node, String)}  must be called.
     *
     * @param node     whose property is about to be removed.
     * @param property about to be removed.
     */
    public void nodePropertyToBeRemoved(Node node, String property) {
        if (commitInProgress) {
            return;
        }

        IdAndKey idAndKey = new IdAndKey(node.getId(), property);

        if (removedNodeProperties.containsKey(idAndKey)) {
            return;
        }

        Object previousValue;
        if (assignedNodeProperties.containsKey(idAndKey)) {
            previousValue = assignedNodeProperties.get(idAndKey).previouslyCommitedValue();
            assignedNodeProperties.remove(idAndKey);

            if (previousValue == null) {
                return;
            }
        } else {
            previousValue = node.getProperty(property, null);
        }

        removedNodeProperties.put(idAndKey, PropertyEntryImpl.removed(node, property, previousValue));
    }

    /**
     * Inform this object about a node's property has been removed. Before this is called,
     * {@link #nodePropertyToBeRemoved(org.neo4j.graphdb.Node, String)} must be called.
     *
     * @param node     whose property has been removed.
     * @param property removed.
     */
    public void nodePropertyRemoved(Node node, String property) {
        if (commitInProgress) {
            return;
        }

        incrementMutationsAndCommitIfNeeded();
    }

    /**
     * Inform this object about a relationship's property about to be removed.  After it has been done,
     * {@link #relationshipPropertyRemoved(org.neo4j.graphdb.Relationship, String)}  must be called.
     *
     * @param relationship whose property is about to be removed.
     * @param property     about to be removed.
     */
    public void relationshipPropertyToBeRemoved(Relationship relationship, String property) {
        if (commitInProgress) {
            return;
        }

        IdAndKey idAndKey = new IdAndKey(relationship.getId(), property);

        if (removedRelationshipProperties.containsKey(idAndKey)) {
            return;
        }

        Object previousValue;
        if (assignedRelationshipProperties.containsKey(idAndKey)) {
            previousValue = assignedRelationshipProperties.get(idAndKey).previouslyCommitedValue();
            assignedRelationshipProperties.remove(idAndKey);

            if (previousValue == null) {
                return;
            }
        } else {
            previousValue = relationship.getProperty(property, null);
        }

        removedRelationshipProperties.put(idAndKey, PropertyEntryImpl.removed(relationship, property, previousValue));
    }

    /**
     * Inform this object about a relationship's property has been removed.  Before this is called,
     * {@link #relationshipPropertyToBeRemoved(org.neo4j.graphdb.Relationship, String)} must be called.
     *
     * @param relationship whose property has been removed.
     * @param property     removed.
     */
    public void relationshipPropertyRemoved(Relationship relationship, String property) {
        if (commitInProgress) {
            return;
        }

        incrementMutationsAndCommitIfNeeded();
    }

    /**
     * Key for maps, containing ID of a property container and key of a property.
     */
    private class IdAndKey {
        private final long id;
        private final String key;

        private IdAndKey(long id, String key) {
            this.id = id;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IdAndKey idAndKey = (IdAndKey) o;

            if (id != idAndKey.id) return false;
            if (!key.equals(idAndKey.key)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + key.hashCode();
            return result;
        }
    }
}