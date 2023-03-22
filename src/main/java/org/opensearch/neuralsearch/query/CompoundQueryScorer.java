/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import lombok.Getter;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

public class CompoundQueryScorer extends Scorer {
    @Getter
    // private final List<Scorer> subScorers;
    private final Scorer[] subScorers;

    private final DisiPriorityQueue subScorersPQ;

    private final DocIdSetIterator approximation;

    // List<Float> subScores;
    float[] subScores;

    Map<Query, Integer> queryToIndex;

    // CompoundQueryScorer(Weight weight, List<Scorer> subScorers, ScoreMode scoreMode) throws IOException {
    CompoundQueryScorer(Weight weight, Scorer[] subScorers, ScoreMode scoreMode) throws IOException {
        super(weight);
        this.subScorers = subScorers;
        queryToIndex = new HashMap<>();
        subScores = new float[subScorers.length];
        int idx = 0;
        int size = 0;
        for (Scorer scorer : subScorers) {
            if (scorer == null) {
                idx++;
                continue;
            }
            queryToIndex.put(scorer.getWeight().getQuery(), idx);
            idx++;
            size++;
        }
        this.subScorersPQ = new DisiPriorityQueue(size);
        for (Scorer scorer : subScorers) {
            if (scorer == null) {
                continue;
            }
            final DisiWrapper w = new DisiWrapper(scorer);
            this.subScorersPQ.add(w);
        }
        this.approximation = new DisjunctionDISIApproximation(this.subScorersPQ);
    }

    public float score() throws IOException {
        // DisiWrapper topList = subScorersPQ.topList();
        float scoreMax = 0;
        double otherScoreSum = 0;
        subScores = new float[subScores.length];
        // for (DisiWrapper w = topList; w != null; w = w.next) {
        Iterator<DisiWrapper> disiWrapperIterator = subScorersPQ.iterator();
        while (disiWrapperIterator.hasNext()) {
            DisiWrapper w = disiWrapperIterator.next();
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            if (w.scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                // subScores.add(0.0f);
                continue;
            }
            float subScore = w.scorer.score();
            subScores[queryToIndex.get(w.scorer.getWeight().getQuery())] = subScore;
            if (subScore >= scoreMax) {
                otherScoreSum += scoreMax;
                scoreMax = subScore;
            } else {
                otherScoreSum += subScore;
            }
        }
        return (float) (scoreMax + otherScoreSum);
    }

    public float[] compoundScores() throws IOException {
        // Map<Query, Float> scores = new HashMap<>();
        float[] scores = new float[subScores.length];
        // for (DisiWrapper disiWrapper : subScorersPQ) {
        DisiWrapper topList = subScorersPQ.topList();
        for (DisiWrapper disiWrapper = topList; disiWrapper != null; disiWrapper = disiWrapper.next) {
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            if (disiWrapper.scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                // scores.put(disiWrapper.scorer.getWeight().getQuery(), 0.0f);
                continue;
            }
            float subScore = disiWrapper.scorer.score();
            scores[queryToIndex.get(disiWrapper.scorer.getWeight().getQuery())] = subScore;
        }
        return scores;
    }

    /*public List<Float> getSubScores() {
        return Collections.unmodifiableList(subScores);
    }*/

    DisiWrapper getSubMatches() throws IOException {
        return subScorersPQ.topList();
    }

    @Override
    public DocIdSetIterator iterator() {
        return approximation;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        float scoreMax = 0;
        double otherScoreSum = 0;
        for (Scorer scorer : subScorers) {
            if (scorer.docID() <= upTo) {
                float subScore = scorer.getMaxScore(upTo);
                if (subScore >= scoreMax) {
                    otherScoreSum += scoreMax;
                    scoreMax = subScore;
                } else {
                    otherScoreSum += subScore;
                }
            }
        }

        return scoreMax;
    }

    @Override
    public int docID() {
        return subScorersPQ.top().doc;
    }
}
