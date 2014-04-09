package org.apache.lucene.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.HashMap;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.fieldvisitor.AllFieldsVisitor;
import org.apache.commons.codec.binary.Base64;
/**
 * A {@link Collector} implementation that collects the top-scoring hits,
 * returning them as a {@link TopDocs}. This is used by {@link IndexSearcher} to
 * implement {@link TopDocs}-based search. Hits are sorted by score descending
 * and then (when the scores are tied) docID ascending. When you create an
 * instance of this collector you should know in advance whether documents are
 * going to be collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and
 * {@link Float#NEGATIVE_INFINITY} are not valid scores.  This
 * collector will not properly collect hits with such
 * scores.
 */
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  // Assumes docs are scored in order.
  private static class InOrderTopScoreDocCollector extends TopScoreDocCollector {
    private InOrderTopScoreDocCollector(int numHits) {
      super(numHits);
    }
    
    @Override
    public void collect(int doc) throws IOException {
       float score = 0;
       BytesRef termValue =  term.bytes();
       String field = term.field();
       // features = null;
       if(features == null)
       {
            score = scorer.score();
           // read the content based on document id
            org.elasticsearch.index.fieldvisitor.FieldsVisitor visitor = new AllFieldsVisitor();
            try {
                context.reader().document(doc + docBase, visitor);
            } catch (Exception e) {
                //logger.("failed to collect doc", e);
                return;
            }
            // decode feature and target from base64 to byte.
           String[] parts = visitor.source().toUtf8().split("\"");
            byte[] feature = Base64.decodeBase64(parts[3]);
            byte[] target = Base64.decodeBase64(termValue.bytes);
            // calculate the L2 distance using the decoded value
            int length = target.length/4;
            float distance = 0;
            if(feature.length >= target.length)
            {
                for(int i = 0; i < length; i++)
                {
                    int j = i*4;
                    int asInt = (feature[j+0] & 0xFF)
                            | ((feature[j+1] & 0xFF) << 8)
                            | ((feature[j+2] & 0xFF) << 16)
                            | ((feature[j+3] & 0xFF) << 24);
                    float eleFeature = Float.intBitsToFloat(asInt);
                    asInt = (target[j+0] & 0xFF)
                            | ((target[j+1] & 0xFF) << 8)
                            | ((target[j+2] & 0xFF) << 16)
                            | ((target[j+3] & 0xFF) << 24);
                    float eleTarget = Float.intBitsToFloat(asInt);

                    distance += (eleFeature - eleTarget)*(eleFeature - eleTarget);
                }
            }
            // scoring by distance
            if(distance < 10 )
            {
                score = 10 - distance;
            }
           // This collector cannot handle these scores:
           assert score != Float.NEGATIVE_INFINITY;
           assert !Float.isNaN(score);

           totalHits++;
           if (score <= pqTop.score) {
               // Since docs are returned in-order (i.e., increasing doc Id), a document
               // with equal score to pqTop.score cannot compete since HitQueue favors
               // documents with lower doc Ids. Therefore reject those docs too.
               return;
           }

           pqTop.doc = doc + docBase;
           pqTop.score = score;
           pqTop = pq.updateTop();
       }
       else
       {
           byte[] target = null;
           if(field.equals("message"))
           {
               target = Base64.decodeBase64(termValue.bytes);
           }
           else if(field.equals("id"))
           {
               if(filterTerm.field().equals("_type"))
               {
                   String keyValue = filterTerm.bytes().utf8ToString()+"#"+termValue.utf8ToString();
                   org.apache.lucene.index.Term term = new org.apache.lucene.index.Term("_uid",keyValue);
                   int docID = org.elasticsearch.common.lucene.uid.Versions.loadRealDocId(context.reader(), term);
                   org.elasticsearch.index.fieldvisitor.FieldsVisitor visitor = new AllFieldsVisitor();
                   context.reader().document(docID, visitor);

                   // decode feature and target from base64 to byte.
                   String[] parts = visitor.source().toUtf8().split("\"");
                   target = Base64.decodeBase64(parts[3]);
               }
           }
           if(target == null)
           {
               return;
           }
           int length = target.length/4;
           float[] targetFeature = new float[length];
           for(int i = 0; i < length; i++)
           {
               int j = i*4;
               int asInt = (target[j+0] & 0xFF)
                       | ((target[j+1] & 0xFF) << 8)
                       | ((target[j+2] & 0xFF) << 16)
                       | ((target[j+3] & 0xFF) << 24);
               targetFeature[i] = Float.intBitsToFloat(asInt);
           }

           java.util.Map<String, float[]> map= java.util.Collections.synchronizedMap(features);

           synchronized(map)
           {
               for (java.util.Map.Entry<String, float[]> entry : map.entrySet()) {
                   String key = entry.getKey();
                   float[] value = entry.getValue();
                   float distance = 0;
                   if(value.length >= targetFeature.length)
                   {
                       for(int i = 0; i < length; i++)
                       {
                           float dis = targetFeature[i] - value[i];
                           distance += dis*dis;
                       }
                   }
                   else
                   {
                       continue;
                   }
                   // scoring by distance
                   if(distance < 10 )
                   {
                       score = 10 - distance;
                   }

                   // This collector cannot handle these scores:
                   assert score != Float.NEGATIVE_INFINITY;
                   assert !Float.isNaN(score);

                   totalHits++;
                   if (score <= pqTop.score) {
                       // Since docs are returned in-order (i.e., increasing doc Id), a document
                       // with equal score to pqTop.score cannot compete since HitQueue favors
                       // documents with lower doc Ids. Therefore reject those docs too.
                       continue;
                   }

                   pqTop.doc = -1;
                   pqTop.score = score;
                   pqTop.key = key;
                   pqTop = pq.updateTop();
               }
           }
           // get docId from key;
           final ScoreDoc[] scoreDocs = new ScoreDoc[pq.size()];
           for (int i = pq.size() - 1; i >= 0; i--) // put docs in array
           {
               scoreDocs[i] = pq.pop();
               org.apache.lucene.index.Term term = new org.apache.lucene.index.Term("_uid",scoreDocs[i].key);
               scoreDocs[i].doc = org.elasticsearch.common.lucene.uid.Versions.loadRealDocId(context.reader(), term);
           }
           pq.clear();
           for(int i = scoreDocs.length-1; i>=0;i--)
           {
               pq.add(scoreDocs[i]);
           }
       }

    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }
  }
  
  // Assumes docs are scored in order.
  private static class InOrderPagingScoreDocCollector extends TopScoreDocCollector {
    private final ScoreDoc after;
    // this is always after.doc - docBase, to save an add when score == after.score
    private int afterDoc;
    private int collectedHits;

    private InOrderPagingScoreDocCollector(ScoreDoc after, int numHits) {
      super(numHits);
      this.after = after;
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();

      // This collector cannot handle these scores:
      assert score != Float.NEGATIVE_INFINITY;
      assert !Float.isNaN(score);

      totalHits++;
      
      if (score > after.score || (score == after.score && doc <= afterDoc)) {
        // hit was collected on a previous page
        return;
      }
      
      if (score <= pqTop.score) {
        // Since docs are returned in-order (i.e., increasing doc Id), a document
        // with equal score to pqTop.score cannot compete since HitQueue favors
        // documents with lower doc Ids. Therefore reject those docs too.
        return;
      }
      collectedHits++;
      pqTop.doc = doc + docBase;
      pqTop.score = score;
      pqTop = pq.updateTop();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
      super.setNextReader(context);
      afterDoc = after.doc - docBase;
    }

    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }
    
    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null ? new TopDocs(totalHits, new ScoreDoc[0], Float.NaN) : new TopDocs(totalHits, results);
    }
  }

  // Assumes docs are scored out of order.
  private static class OutOfOrderTopScoreDocCollector extends TopScoreDocCollector {
    private OutOfOrderTopScoreDocCollector(int numHits) {
      super(numHits);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();

      // This collector cannot handle NaN
      assert !Float.isNaN(score);

      totalHits++;
      if (score < pqTop.score) {
        // Doesn't compete w/ bottom entry in queue
        return;
      }
      doc += docBase;
      if (score == pqTop.score && doc > pqTop.doc) {
        // Break tie in score by doc ID:
        return;
      }
      pqTop.doc = doc;
      pqTop.score = score;
      pqTop = pq.updateTop();
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  }
  
  // Assumes docs are scored out of order.
  private static class OutOfOrderPagingScoreDocCollector extends TopScoreDocCollector {
    private final ScoreDoc after;
    // this is always after.doc - docBase, to save an add when score == after.score
    private int afterDoc;
    private int collectedHits;

    private OutOfOrderPagingScoreDocCollector(ScoreDoc after, int numHits) {
      super(numHits);
      this.after = after;
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();

      // This collector cannot handle NaN
      assert !Float.isNaN(score);

      totalHits++;
      if (score > after.score || (score == after.score && doc <= afterDoc)) {
        // hit was collected on a previous page
        return;
      }
      if (score < pqTop.score) {
        // Doesn't compete w/ bottom entry in queue
        return;
      }
      doc += docBase;
      if (score == pqTop.score && doc > pqTop.doc) {
        // Break tie in score by doc ID:
        return;
      }
      collectedHits++;
      pqTop.doc = doc;
      pqTop.score = score;
      pqTop = pq.updateTop();
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
    
    @Override
    public void setNextReader(AtomicReaderContext context) {
      super.setNextReader(context);
      afterDoc = after.doc - docBase;
    }
    
    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }
    
    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null ? new TopDocs(totalHits, new ScoreDoc[0], Float.NaN) : new TopDocs(totalHits, results);
    }
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect and whether documents are scored in order by the input
   * {@link Scorer} to {@link #setScorer(Scorer)}.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, boolean docsScoredInOrder) {
    return create(numHits, null, docsScoredInOrder);
  }
  
  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect, the bottom of the previous page, and whether documents are scored in order by the input
   * {@link Scorer} to {@link #setScorer(Scorer)}.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, ScoreDoc after, boolean docsScoredInOrder) {
    
    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }
    
    if (docsScoredInOrder) {
      return after == null 
        ? new InOrderTopScoreDocCollector(numHits) 
        : new InOrderPagingScoreDocCollector(after, numHits);
    } else {
      return after == null
        ? new OutOfOrderTopScoreDocCollector(numHits)
        : new OutOfOrderPagingScoreDocCollector(after, numHits);
    }
    
  }
  
  ScoreDoc pqTop;
  int docBase = 0;
  Scorer scorer;
  IndexReaderContext context;
  Term term;
  Term filterTerm;
  public HashMap<String,float[]> features = null;
  // prevents instantiation
  private TopScoreDocCollector(int numHits) {
    super(new HitQueue(numHits, true));
    // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
    // that at this point top() is already initialized.
    pqTop = pq.top();
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }
    
    // We need to compute maxScore in order to set it in TopDocs. If start == 0,
    // it means the largest element is already in results, use its score as
    // maxScore. Otherwise pop everything else, until the largest element is
    // extracted and use its score as maxScore.
    float maxScore = Float.NaN;
    if (start == 0) {
      maxScore = results[0].score;
    } else {
      for (int i = pq.size(); i > 1; i--) { pq.pop(); }
      maxScore = pq.pop().score;
    }
    
    return new TopDocs(totalHits, results, maxScore);
  }
  
  @Override
  public void setNextReader(AtomicReaderContext context) {
    docBase = context.docBase;
  }

  public void setContext(IndexReaderContext context) {
        this.context = context;
  }

  public void setTermValue(Term termValue) {
        this.term = termValue;
  }

  public void setTermFilterValue (Term term) { this.filterTerm = term; };

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }
}
