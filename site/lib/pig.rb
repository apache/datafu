# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require 'rouge'

class Pig < Rouge::RegexLexer
  desc "Pig"
  tag 'pig'
  filenames '*.pig'
  mimetypes 'text/x-pig'

  def self.keywords
    @keywords ||= Set.new %w(
      ASSERT COGROUP CROSS DEFINE DISTINCT FILTER
      FOREACH GROUP IMPORT JOIN LIMIT LOAD MAPREDUCE
      ORDER BY SAMPLE SPLIT STORE STREAM UNION
      GENERATE ALL DUMP AS REGISTER USING ASC DESC ANY
      FULL INNER OUTER EXEC DESCRIBE CASE EXPLAIN
      ILLUSTRATE IS INTO IF LEFT RIGHT MATCHES PARALLEL
      ROLLUP SHIP AND OR NOT

      AVG MIN MAX SIZE TOKENIZE FLATTEN RANK CUBE COUNT
      CONCAT SUM SQRT COUNT_STAR
      )
  end

  state :root do
    rule /\s+/m, Text
    rule /--.*?\n/, Comment::Single
    rule %r(/\*), Comment::Multiline, :multiline_comments
    rule /\d+/, Num::Integer
    rule /'/, Str::Single, :single_string
    rule /"/, Name::Variable, :double_string
    rule /`/, Name::Variable, :backtick

    rule /[$]?\w[\w\d]*/ do |m|
      if self.class.keywords.include? m[0].upcase
        token Keyword
      else
        token Name
      end
    end

    rule %r([+*/<>=~!@#%^&|?^-]), Operator
    rule /[;:(){}\[\],.]/, Punctuation
  end

  state :multiline_comments do
    rule %r(/[*]), Comment::Multiline, :multiline_comments
    rule %r([*]/), Comment::Multiline, :pop!
    rule %r([^/*]+), Comment::Multiline
    rule %r([/*]), Comment::Multiline
  end

  state :backtick do
    rule /\\./, Str::Escape
    rule /``/, Str::Escape
    rule /`/, Name::Variable, :pop!
    rule /[^\\`]+/, Name::Variable
  end

  state :single_string do
    rule /\\./, Str::Escape
    rule /''/, Str::Escape
    rule /'/, Str::Single, :pop!
    rule /[^\\']+/, Str::Single
  end

  state :double_string do
    rule /\\./, Str::Escape
    rule /""/, Str::Escape
    rule /"/, Name::Variable, :pop!
    rule /[^\\"]+/, Name::Variable
  end
end
