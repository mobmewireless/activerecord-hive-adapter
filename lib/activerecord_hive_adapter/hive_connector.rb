module ActiveRecordHiveAdapter

  class HiveConnector
    attr_reader :database

    def initialize(host, port, database, timeout)
      socket = Thrift::Socket.new(host, port, timeout)
      @transport = Thrift::BufferedTransport.new(socket)
      protocol = Thrift::BinaryProtocol.new(@transport)
      @client = ThriftHive::Client.new(protocol)
      open(database)
    end

    def open(database)
      @transport.open
      self.database = database
    end

    def reconnect!
      close
      open(self.database)
    end

    def close
      @transport.close rescue IOError
    end

    def open?
      @transport.open?
    end

    def execute(sql)
      @client.execute(sql)
    end

    def fetch_all
      @client.fetchAll()
    end

    def get_schema
      @client.getSchema()
    end

    def database=(new_database)
      execute("USE #{new_database}")
      @database = new_database
    end
  end
end
