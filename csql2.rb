require 'csv'
require 'open3'
require 'csv'
require 'sql-parser'
class CSQL
  def initialize(filepath)
    @parser = SQLParser::Parser.new
    @filepath = filepath
  end

  def execute query
    begin
      modified_query = query.gsub(/csvfile/, @filepath)
      result,err,process = Open3.capture3("q -H -d \',\' \'#{modified_query}\'")
      if err != ""
        raise CSQLException.new(err)
      end
    end
    ast = @parser.scan_str(query)
    column = ast.query_expression.list.to_sql
    columns = nil
    if column == "*"
      columns = CSV.table(@filepath).headers.map(&:to_s)
    else
      columns = column.chomp.split(',').map{|c|c.chomp}
    end
    result = result.chomp.split("\n").map!{|r|
      data = r.split(",")
      hash = Hash.new
      data.size.times do |i|
        hash[columns[i]] = data[i]
      end
      return hash
    }
    return result
  end
end

class CSQLException < Exception
end

p CSQL.new('./test.csv').execute 'select * from csvfile'
