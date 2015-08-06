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
         default: 28_015

  option :authkey,
         description: 'RethinkDB Auth Key',
         short: '-a AUTHKEY',
         long: '--authkey AUTHKEY'

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
      result = r.db('rethinkdb').table('server_status')[:process].run(conn)
      version = result.next['version']
      ok "Server version: #{version}"
    rescue RethinkDB::RqlDriverError => e
      critical "Error message: #{e.error}"
    ensure
      conn.close if conn
    end
  end
end
