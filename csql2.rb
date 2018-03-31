require 'csv'
require 'open3'
require 'sql-parser'
require 'csv'

class CSQL
  def initialize()
    @parser = SQLParser::Parser.new
  end

  def execute query
    begin
      result,err,process = Open3.capture3("q -H -d \',\' \'#{query}\'")
      if err != ""
        raise CSQLException.new(err)
      end
    end
    ast = @parser.scan_str(query)
    column = ast.query_expression.list.to_sql
    columns = nil
    if column == "*"
      table = ast.query_expression.table_expression.from_clause.to_sql
      columns = CSV.table(table).headers.map(&:to_s)
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

p CSQL.new.execute 'select * from test.csv'
