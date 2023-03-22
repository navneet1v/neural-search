/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

public class CompoundQuery extends Query implements Iterable<Query> {
    // it's of type Multiset in OS implementation, using list to keep original order
    private final List<Query> subQueries = new ArrayList<>();

    public CompoundQuery(Collection<Query> subQueries) {
        Objects.requireNonNull(subQueries, "Collection of Querys must not be null");
        this.subQueries.addAll(subQueries);
    }

    @Override
    public Iterator<Query> iterator() {
        return getSubQueries().iterator();
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("(");
        Iterator<Query> it = subQueries.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Query subquery = it.next();
            if (subquery instanceof BooleanQuery) { // wrap sub-bools in parens
                buffer.append("(");
                buffer.append(subquery.toString(field));
                buffer.append(")");
            } else buffer.append(subquery.toString(field));
            if (i != subQueries.size() - 1) buffer.append(" | ");
        }
        buffer.append(")");
        return buffer.toString();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (subQueries.isEmpty()) {
            return new MatchNoDocsQuery("empty CompoundQuery");
        }

        if (subQueries.size() == 1) {
            return subQueries.iterator().next();
        }

        boolean actuallyRewritten = false;
        List<Query> rewrittenSubQueries = new ArrayList<>();
        for (Query sub : subQueries) {
            Query rewrittenSub = sub.rewrite(reader);
            actuallyRewritten |= rewrittenSub != sub;
            rewrittenSubQueries.add(rewrittenSub);
        }

        if (actuallyRewritten) {
            return new CompoundQuery(rewrittenSubQueries);
        }

        return super.rewrite(reader);
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        for (Query q : subQueries) {
            q.visit(v);
        }
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(CompoundQuery other) {
        return Objects.equals(subQueries, other.subQueries);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + Objects.hashCode(subQueries);
        return h;
    }

    public Collection<Query> getSubQueries() {
        return Collections.unmodifiableCollection(subQueries);
    }

    protected class CompoundQueryWeight extends Weight {

        int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

        /** The Weights for our subqueries, in 1-1 correspondence */
        protected final ArrayList<Weight> weights = new ArrayList<>(); // The Weight's for our subqueries, in 1-1 correspondence

        private final ScoreMode scoreMode;

        /**
         * Construct the Weight for this Query searched by searcher. Recursively construct subquery
         * weights.
         */
        public CompoundQueryWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            super(CompoundQuery.this);
            for (Query query : subQueries) {
                weights.add(searcher.createWeight(query, scoreMode, boost));
            }
            this.scoreMode = scoreMode;
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            List<Matches> mis = new ArrayList<>();
            for (Weight weight : weights) {
                Matches mi = weight.matches(context, doc);
                if (mi != null) {
                    mis.add(mi);
                }
            }
            return MatchesUtils.fromSubMatches(mis);
        }

        /** Create the scorer used to score our associated Query */
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer[] scorers = new Scorer[weights.size()];
            for (int i = 0; i < weights.size(); i++) {
                Weight w = weights.get(i);
                // we will advance() subscorers
                Scorer subScorer = w.scorer(context);
                if (subScorer != null) {
                    scorers[i] = subScorer;
                }
            }
            return new CompoundQueryScorer(this, scorers, scoreMode);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            if (weights.size() > BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
                // Disallow caching large dismax queries to not encourage users
                // to build large dismax queries as a workaround to the fact that
                // we disallow caching large TermInSetQueries.
                return false;
            }
            for (Weight w : weights) {
                if (w.isCacheable(ctx) == false) return false;
            }
            return true;
        }

        /** Explain the score we computed for doc */
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            throw new UnsupportedOperationException("Explian is not supported");
        }
    }

    /** Create the Weight used to score us */

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new CompoundQueryWeight(searcher, scoreMode, boost);
    }
}
