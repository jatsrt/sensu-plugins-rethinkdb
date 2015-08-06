# !/usr/bin/env ruby
#
# RethinkDB Metrics Plugin
# ===
#
# This plugin uses RethinkDB's 'stats' table to collect metrics
# from an instance of RethinkDB.
#
# Copyright 2015 Jake Thompson <jake@prelist.co>
#
# Released under the same terms as Sensu (the MIT license); see LICENSE
# for details.
#

require 'sensu-plugin/metric/cli'
require 'rethinkdb'
include RethinkDB::Shortcuts

class RethinkDBMetrics < Sensu::Plugin::Metric::CLI::Graphite
  option :host,
         description: 'RethinkDB Host',
         short: '-h HOST',
         long: '--host HOST',
         default: '127.0.01'

  option :port,
         description: 'RethinkDB Port',
         short: '-p PORT',
         long: '--port PORT',
         default: 28_015

  option :authkey,
         description: 'RethinkDB Auth Key',
         short: '-a AUTHKEY',
         long: '--authkey AUTHKEY'

  option :scheme,
         description: 'Metric naming scheme, text to prepend to metric',
         short: '-s SCHEME',
         long: '--scheme SCHEME',
         default: "#{Socket.gethostname}.rethinkdb"

  def run
    begin
      options = {
        host: config[:host],
        port: config[:port]
      }
      unless config[:authkey].nil?
        options[:auth_key] = config[:authkey]
      end

      conn = r.connect(options)
      results = r.db('rethinkdb').table('stats').run(conn)
    rescue RethinkDB::RqlDriverError => e
      puts e.message
    end

    results.each do |doc|
      timestamp = Time.now.to_i
      type = doc['id'][0]

      case type
      when 'server'
        key = "#{type}.#{doc['server']}"
      when 'table'
        key = "#{type}.#{doc['db']}.#{doc['table']}"
      when 'table_server'
        key = "#{type}.#{doc['server']}.#{doc['db']}.#{doc['table']}"
      else
        key = type
      end

      doc['query_engine'].unnest.each do |metric, value|
        output [config[:scheme], key, 'query', metric].join('.'), value, timestamp
      end

      unless doc['storage_engine'].nil?
        doc['storage_engine'].unnest.each do |metric, value|
          output [config[:scheme], key, 'storage', metric].join('.'), value, timestamp
        end
      end
    end
    ok
  end
end

class Hash
  def unnest
    new_hash = {}
    each do |key, val|
      if val.is_a?(Hash)
        new_hash.merge!(val.prefix_keys("#{key}."))
      else
        new_hash[key] = val
      end
    end
    new_hash
  end

  def prefix_keys(prefix)
    Hash[map { |key, val| [prefix + key, val] }].unnest
  end
end
