# !/usr/bin/env ruby
#
# RethinkDB Alive Plugin
# ===
#
# This plugin attempts to login to rethinkdb with provided credentials.
#
# Copyright 2015 Jake Thompson <jake@prelist.co>
#
# Released under the same terms as Sensu (the MIT license); see LICENSE
# for details.

require 'sensu-plugin/check/cli'
require 'json'
require 'rethinkdb'
include RethinkDB::Shortcuts

class CheckRethinkDB < Sensu::Plugin::Check::CLI
  option :host,
         description: 'RethinkDB Host',
         short: '-h HOST',
         long: '--host HOST',
         default: '127.0.01'

  option :port,
         description: 'RethinkDB Port',
         short: '-p PORT',
         long: '--port PORT',
         proc: proc(&:to_i),
         default: 28_015

  option :authkey,
         description: 'RethinkDB Auth Key',
         short: '-a AUTHKEY',
         long: '--authkey AUTHKEY'

  option :database,
         description: 'RethinkDB Database',
         short: '-d DB',
         long: '--database DB',
         default: 'test'

  option :table,
         description: 'RethinkDB Table',
         short: '-t TABLE',
         long: '--table TABLE',
         default: 'test'

  option :warn,
         short: '-w WARN',
         proc: proc(&:to_f),
         default: 1

  option :crit,
         short: '-c CRIT',
         proc: proc(&:to_f),
         default: 2

  option :filter,
         description: 'RethinkDB Filter as a JSON String',
         short: '-f FILTER',
         long: '--filter FILTER'

  def run
    begin
      conn = r.connect(host: config[:host], port: config[:port])
      if config[:filter].nil?
        table_count = r.db(config[:database]).table(config[:table]).count.run(conn)
      else
        filter = JSON.parse(config[:filter])
        table_count = r.db(config[:database]).table(config[:table]).filter(filter).count.run(conn)
      end

      msg = "total=#{table_count}"

      message msg

      critical if table_count > config[:crit]
      warning if table_count > config[:warn]
      ok
    rescue RethinkDB::RqlDriverError => e
      critical "Error message: #{e.error}"
    ensure
      conn.close if conn
    end
  end
end
