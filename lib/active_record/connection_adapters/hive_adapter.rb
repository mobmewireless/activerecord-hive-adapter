module Arel
  module Visitors
    class Hive < Arel::Visitors::ToSql

    end
  end
end

module ActiveRecord
  class Base
    def self.hive_connection(config)
      connection_params = config.values_at(:host, :port, :database, :timeout)
      ConnectionAdapters::HiveAdapter.new(nil, logger, connection_params, config)
    end
  end

  module ConnectionAdapters
    class HiveColumn < Column
      def initialize(name, default, sql_type, partition)
        super(name, default, sql_type)
        @partition = partition
      end

      def partition?
        @partition
      end

      def extract_default(default)
        case [type, default]
        when [:date, 'current_date'] then return default
        when [:datetime, 'current_time'] then return default
        end
        super
      end

      def realized_default
        # returns realized value of defaults
        case [type, default]
        when [:date, 'current_date'] then Date.today
        when [:datetime, 'current_time'] then DateTime.now
        else default
        end
      end
    end

    class HiveAdapter < AbstractAdapter

      module TableDefinitionExtensions
        attr_reader :partitions

        def partition(name, type, options={ })
          column(name, type, options)
          @partitions = [] unless @partitions
          @partitions << @columns.pop
        end

        def row_format
          'ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"'
        end

        def external
          true
        end
      end

      NATIVE_DATABASE_TYPES = {
        :string      => { :name => "string" },
        :text        => { :name => "string" },
        :integer     => { :name => "int" },
        :float       => { :name => "float" },
        :double      => { :name => "double" },
        :datetime    => { :name => "string" },
        :timestamp   => { :name => "string" },
        :time        => { :name => "string" },
        :date        => { :name => "string" },
        :binary      => { :name => "string" },
        :boolean     => { :name => "tinyint" }
      }

      class BindSubstitution < Arel::Visitors::Hive
        include Arel::Visitors::BindVisitor
      end

      def initialize(connection, logger, connection_params, config)
        super(connection, logger)
        @connection_params = connection_params
        connect
        @visitor = BindSubstitution.new(self)
      end

      def connect
        @connection = ActiveRecordHiveAdapter::HiveConnector.new(*@connection_params)
      end

      def disconnect
        @connection.close
      end

      def reconnect!
        disconnect
        connect
      end

      def active?
        begin
          @connection.execute("SET check=1")
          true
        rescue
          false
        end
      end

      def execute(sql, name=nil)
        with_auto_reconnect do
          log(sql, name) { @connection.execute(sql) }
        end
      end

      def adapter_name #:nodoc:
        'Hive'
      end

      def supports_migrations? #:nodoc:
        true
      end

      def supports_primary_key? #:nodoc:
        false
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      def query(sql, name=nil)
        with_auto_reconnect do
          log(sql, name) do
            @connection.execute(sql)
            @connection.fetch_all
          end
        end
      end

      def select(sql, name=nil, binds=[])
        with_auto_reconnect do
          log(sql, name) do
            @connection.execute(sql)
            fields = @connection.get_schema.fieldSchemas.map { |f| f.name }
            res = @connection.fetch_all
            res.map { |row| Hash[*fields.zip(row.split("\t")).flatten] }
          end
        end
      end

      def select_rows(sql, name=nil)
        query(sql, name)
      end

      def database_name
        @connection.database
      end

      def tables(name=nil)
        query("SHOW TABLES", name)
      end

      def primary_key(table_name)
        nil
      end

      def columns(table, name=nil)
        res = query("DESCRIBE FORMATTED #{quote_table_name(table)}", name)

        table_info_index = res.find_index { |ln| ln.start_with?("# Detailed Table Information") }
        begin_partition = false
        columns = []
        res.slice(0, table_info_index - 1).each do |ln|
          if ln.start_with?("# Partition Information")
            begin_partition = true
            next
          end
          next if ln.strip.empty?
          next if ln.start_with?("# col_name")
          col_name, sql_type, comment = ln.split(/\s+/)
          meta = Hash[comment.to_s.split(',').map { |meta| property, value = meta.split('=') }]
          type = meta['ar_type'] || sql_type
          columns << HiveColumn.new(col_name, meta['ar_default'], type, begin_partition)
        end
        columns
      end

      def create_table(table_name, options={ })
        table_definition = TableDefinition.new(self)
        table_definition.extend(TableDefinitionExtensions)

        yield table_definition if block_given?

        if options[:force] && table_exists?(table_name)
          drop_table(table_name, options)
        end

        create_sql = "CREATE#{' EXTERNAL' if table_definition.external} TABLE "
        create_sql << "#{quote_table_name(table_name)} ("
        create_sql << table_definition.to_sql
        create_sql << ") "
        create_sql << "#{partitioned_by(table_definition.partitions)} "
        create_sql << table_definition.row_format
        execute create_sql
      end

      def add_column_options!(sql, options) #:nodoc:
        meta = ""
        meta << "ar_type=#{options[:column].type}"
        meta << ",ar_default=#{options[:default]}" if options[:default]
        sql << " COMMENT '#{meta}'"
      end

      def add_index(table_name, column_name, options = { })
        raise NotImplementedError
      end

      def add_column(table_name, column_name, type, options = { })
        sql = "ALTER TABLE #{quote_table_name(table_name)} ADD COLUMNS (#{quote_column_name(column_name)} #{type_to_sql(type)}"
        o = { }
        o[:default] = options[:default] if options[:default]
        o[:column]  = HiveColumn.new(column_name, nil, type, false)
        add_column_options!(sql, o)
        sql << ")"
        execute(sql)
      end

      def partitioned_by(partitions)
        unless partitions.to_a.empty?
          spec = "PARTITIONED BY ("
          spec << partitions.map do |p|
            options = { :default => p.default, :column => p }
            add_column_options!("#{p.name} #{p.sql_type}", options)
          end.join(", ") << ")"
        end
      end

      def with_auto_reconnect
        yield
      rescue Thrift::TransportException => e
        raise unless e.message == "end of file reached"
        reconnect!
        yield
      end
    end

  end
end
