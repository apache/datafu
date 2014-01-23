
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
    rule /\s+/m, 'Text'
    rule /--.*?\n/, 'Comment.Single'
    rule %r(/\*), 'Comment.Multiline', :multiline_comments
    rule /\d+/, 'Literal.Number.Integer'
    rule /'/, 'Literal.String.Single', :single_string
    rule /"/, 'Name.Variable', :double_string
    rule /`/, 'Name.Variable', :backtick

    rule /[$]?\w[\w\d]*/ do |m|
      if self.class.keywords.include? m[0].upcase
        token 'Keyword'
      else
        token 'Name'
      end
    end

    rule %r([+*/<>=~!@#%^&|?^-]), 'Operator'
    rule /[;:(){}\[\],.]/, 'Punctuation'
  end

  state :multiline_comments do
    rule %r(/[*]), 'Comment.Multiline', :multiline_comments
    rule %r([*]/), 'Comment.Multiline', :pop!
    rule %r([^/*]+), 'Comment.Multiline'
    rule %r([/*]), 'Comment.Multiline'
  end

  state :backtick do
    rule /\\./, 'Literal.String.Escape'
    rule /``/, 'Literal.String.Escape'
    rule /`/, 'Name.Variable', :pop!
    rule /[^\\`]+/, 'Name.Variable'
  end

  state :single_string do
    rule /\\./, 'Literal.String.Escape'
    rule /''/, 'Literal.String.Escape'
    rule /'/, 'Literal.String.Single', :pop!
    rule /[^\\']+/, 'Literal.String.Single'
  end

  state :double_string do
    rule /\\./, 'Literal.String.Escape'
    rule /""/, 'Literal.String.Escape'
    rule /"/, 'Name.Variable', :pop!
    rule /[^\\"]+/, 'Name.Variable'
  end
end
