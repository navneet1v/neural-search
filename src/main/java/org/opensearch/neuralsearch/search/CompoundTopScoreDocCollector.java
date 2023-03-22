/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.LongAccumulator;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.query.CompoundQueryScorer;

@Log4j2
public class CompoundTopScoreDocCollector<T extends ScoreDoc> implements Collector {
    int docBase;
    float minCompetitiveScore;
    final HitsThresholdChecker hitsThresholdChecker;
    final MaxScoreAccumulator minScoreAcc;
    ScoreDoc pqTop;
    protected TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    protected int[] totalHits;
    public static final TopDocs EMPTY_TOPDOCS = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    int numOfHits;

    @Getter
    PriorityQueue<ScoreDoc>[] compoundScores;

    public CompoundTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
        // this.pq = new HitQueue(numHits, true);
        numOfHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
        this.minScoreAcc = minScoreAcc;
        // pqTop = pq.top();
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        // reset the minimum competitive score
        docBase = context.docBase;
        minCompetitiveScore = 0f;
        // compoundScores.clear();

        return new TopScoreDocCollector.ScorerLeafCollector() {
            CompoundQueryScorer compoundQueryScorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                super.setScorer(scorer);
                if (minScoreAcc == null) {
                    updateMinCompetitiveScore(scorer);
                } else {
                    updateGlobalMinCompetitiveScore(scorer);
                }
                compoundQueryScorer = (CompoundQueryScorer) scorer;
            }

            @Override
            public void collect(int doc) throws IOException {
                float[] subScoresByQuery = compoundQueryScorer.compoundScores();
                // iterate over results for each query
                if (compoundScores == null) {
                    compoundScores = new PriorityQueue[subScoresByQuery.length];
                    for (int i = 0; i < subScoresByQuery.length; i++) {
                        compoundScores[i] = new HitQueue(numOfHits, true);
                    }
                    totalHits = new int[subScoresByQuery.length];
                }
                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    if (score == 0) {
                        continue;
                    }
                    totalHits[i]++;
                    PriorityQueue<ScoreDoc> pq = compoundScores[i];
                    ScoreDoc topDoc = pq.top();
                    topDoc.doc = doc + docBase;
                    topDoc.score = score;
                    pq.updateTop();
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    /*protected int topDocsSize(Query query) {
        // In case pq was populated with sentinel values, there might be less
        // results than pq.size(). Therefore return all results until either
        // pq.size() or totalHits.
        int totalHitsPerQuery = totalHits.get(query);
        // return totalHits < pq.size() ? totalHits : pq.size();
        return totalHitsPerQuery;
    }*/

    protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
        if (hitsThresholdChecker.isThresholdReached() && pqTop != null && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the
                                                                                                                    // score of sentinels
            // since we tie-break on doc id and collect in doc id order, we can require
            // the next float
            float localMinScore = Math.nextUp(pqTop.score);
            if (localMinScore > minCompetitiveScore) {
                scorer.setMinCompetitiveScore(localMinScore);
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                minCompetitiveScore = localMinScore;
                if (minScoreAcc != null) {
                    // we don't use the next float but we register the document
                    // id so that other leaves can require it if they are after
                    // the current maximum
                    minScoreAcc.accumulate(docBase, pqTop.score);
                }
            }
        }
    }

    protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
        assert minScoreAcc != null;
        MaxScoreAccumulator.DocAndScore maxMinScore = minScoreAcc.get();
        if (maxMinScore != null) {
            // since we tie-break on doc id and collect in doc id order we can require
            // the next float if the global minimum score is set on a document id that is
            // smaller than the ids in the current leaf
            float score = docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
            if (score > minCompetitiveScore) {
                assert hitsThresholdChecker.isThresholdReached();
                scorer.setMinCompetitiveScore(score);
                minCompetitiveScore = score;
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
        }
    }

    public TopDocs[] topDocs() {
        TopDocs[] topDocs = new TopDocs[compoundScores.length];
        for (int i = 0; i < compoundScores.length; i++) {
            int qTopSize = totalHits[i];
            TopDocs topDocsPerQuery = topDocsPerQuery(0, Math.min(qTopSize, compoundScores[i].size()), compoundScores[i], qTopSize);
            topDocs[i] = topDocsPerQuery;
        }
        return topDocs;
    }

    TopDocs topDocsPerQuery(int start, int howMany, PriorityQueue<ScoreDoc> pq, int totalHits) {
        // int size = topDocsSize();
        int size = howMany;

        if (howMany < 0) {
            throw new IllegalArgumentException("Number of hits requested must be greater than 0 but value was " + howMany);
        }

        if (start < 0) {
            throw new IllegalArgumentException("Expected value of starting position is between 0 and " + size + ", got " + start);
        }

        if (start >= size || howMany == 0) {
            return newTopDocs(null, start, totalHits);
        }

        howMany = Math.min(size - start, howMany);
        ScoreDoc[] results = new ScoreDoc[howMany];
        // pq's pop() returns the 'least' element in the queue, therefore need
        // to discard the first ones, until we reach the requested range.
        // Note that this loop will usually not be executed, since the common usage
        // should be that the caller asks for the last howMany results. However it's
        // needed here for completeness.
        for (int i = pq.size() - start - howMany; i > 0; i--) {
            pq.pop();
        }

        // Get the requested results from pq.
        populateResults(results, howMany, pq);

        return newTopDocs(results, start, totalHits);
    }

    protected void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<ScoreDoc> pq) {
        for (int i = howMany - 1; i >= 0; i--) {
            results[i] = pq.pop();
        }
    }

    protected TopDocs newTopDocs(ScoreDoc[] results, int start, int totalHits) {
        return results == null ? EMPTY_TOPDOCS : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }
}

final class MaxScoreAccumulator {
    // we use 2^10-1 to check the remainder with a bitwise operation
    static final int DEFAULT_INTERVAL = 0x3ff;

    // scores are always positive
    final LongAccumulator acc = new LongAccumulator(MaxScoreAccumulator::maxEncode, Long.MIN_VALUE);

    // non-final and visible for tests
    long modInterval;

    MaxScoreAccumulator() {
        this.modInterval = DEFAULT_INTERVAL;
    }

    private static long maxEncode(long v1, long v2) {
        float score1 = Float.intBitsToFloat((int) (v1 >> 32));
        float score2 = Float.intBitsToFloat((int) (v2 >> 32));
        int cmp = Float.compare(score1, score2);
        if (cmp == 0) {
            // tie-break on the minimum doc base
            return (int) v1 < (int) v2 ? v1 : v2;
        } else if (cmp > 0) {
            return v1;
        }
        return v2;
    }

    void accumulate(int docBase, float score) {
        assert docBase >= 0 && score >= 0;
        long encode = (((long) Float.floatToIntBits(score)) << 32) | docBase;
        acc.accumulate(encode);
    }

    MaxScoreAccumulator.DocAndScore get() {
        long value = acc.get();
        if (value == Long.MIN_VALUE) {
            return null;
        }
        float score = Float.intBitsToFloat((int) (value >> 32));
        int docBase = (int) value;
        return new MaxScoreAccumulator.DocAndScore(docBase, score);
    }

    static class DocAndScore implements Comparable<MaxScoreAccumulator.DocAndScore> {
        final int docBase;
        final float score;

        DocAndScore(int docBase, float score) {
            this.docBase = docBase;
            this.score = score;
        }

        public int compareTo(MaxScoreAccumulator.DocAndScore o) {
            int cmp = Float.compare(score, o.score);
            if (cmp == 0) {
                return Integer.compare(o.docBase, docBase);
            }
            return cmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MaxScoreAccumulator.DocAndScore result = (MaxScoreAccumulator.DocAndScore) o;
            return docBase == result.docBase && Float.compare(result.score, score) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(docBase, score);
        }

        @Override
        public String toString() {
            return "DocAndScore{" + "docBase=" + docBase + ", score=" + score + '}';
        }
    }
}
