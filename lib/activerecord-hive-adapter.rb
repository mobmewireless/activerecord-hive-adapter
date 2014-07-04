require_relative 'activerecord_hive_adapter/version.rb'

module ActiverecordHiveAdapter
  require 'thrift_hive'
  require 'arel/visitors/bind_visitor'

  require_relative 'activerecord_hive_adapter/hive_connector'
end
