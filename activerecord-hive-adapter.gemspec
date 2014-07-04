# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "activerecord_hive_adapter/version"

Gem::Specification.new do |s|
  s.name        = "activerecord-hive-adapter"
  s.version     = ActiverecordHiveAdapter::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Nanda Sankaran"]
  s.email       = ["nanda@mobme.in"]
  s.homepage    = ""
  s.summary     = %q{Hive adapter for ActiveRecord}
  s.description = %q{Hive adapter for ActiveRecord}

  s.rubyforge_project = 'activerecord-hive-adapter'

  s.add_dependency('thrift')
  s.add_dependency('arel')

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib", "vendor/thrift_hive/gen-rb"]
end
