/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Given a set of documents, returns tf-idf feature vectors for those documents.
 *
 * documents:   { id, text:chararray }  Document set.
 * maxFeatures: int                     Maximum number of features to return per document
 * ==>
 * vectors: { id, features:{(token:chararray, weight:float)} } Ordered by weight desc.
 */
define DataFu_NlpTFIDF(documents, maxFeatures) returns vectors {

  define TokenizeSimple datafu.pig.text.opennlp.TokenizeSimple();

  --
  -- Get corpus size first
  --
  uniq     = distinct (foreach $documents generate id);
  num_docs = foreach (group uniq all) generate COUNT(uniq) as N; -- ugh.

  --
  -- Tokenize the documents
  --
  tokenized = foreach $documents generate
                id,
                flatten(TokenizeSimple(text)) as (token:chararray);

  --
  -- Next, get raw term frequencies. Combiners will be made use of here to reduce some of the
  -- token explosion
  --
  term_freqs = foreach (group tokenized by (id, token)) generate
                 flatten(group)   as (id, token),
                 COUNT(tokenized) as term_freq;

  --
  -- Now, compute the 'augmented' frequency to prevent bias toward long docs
  --
  max_term_freqs = foreach (group term_freqs by id) generate
                     flatten(term_freqs)       as (id, token, term_freq),
                     MAX(term_freqs.term_freq) as max_term_freq;

  aug_term_freqs = foreach max_term_freqs {
                     -- see: http://www.cs.odu.edu/~jbollen/IR04/readings/article1-29-03.pdf
                     aug_freq = 0.5f + (0.5f * term_freq)/max_term_freq;
                     generate
                       id       as id,
                       token    as token,
                       aug_freq as term_freq;
                    };

  --
  -- Next, get document frequency; how many documents does a term appear in.
  --
  doc_freqs = foreach (group aug_term_freqs by token) {
                raw_doc_freq = COUNT(aug_term_freqs);
                idf          = LOG((float)num_docs.N/(float)raw_doc_freq);
                generate
                  flatten(aug_term_freqs) as (id, token, term_freq),
                  idf                     as idf;
              };

  --
  -- Finally, compute tf-idf
  --
  weights = foreach doc_freqs generate
              id            as id,
              token         as token,
              term_freq*idf as weight;

  $vectors = foreach (group weights by id) {
               ordered = order weights by weight desc;
               top_N   = limit ordered $maxFeatures; -- use this instead of top to maintain ordering
               generate
                 group                as id,
                 top_N.(token,weight) as features;
             };
};