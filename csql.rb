require 'csv'
require 'sqlite3'

class CSQL
  def initialize(filenames)
    @db = SQLite3::Database.new 'data.db'
    @db.results_as_hash = true
    @db.transaction do
      filenames.each do |filename|
        file = CSV.table(filename)
        headers = file.headers.map{|header| header + " TEXT" }
        schema = headers.join(', ')
        @db.execute "CREATE TABLE ? ( ? ) ", filename.gsub(/\.csv/,''), schema

      end
    end
  end

  def sql query
    @db.execute query
  end
end

csql = CSQL.new(["test.csv"])
p csql.sql("select * from test")
