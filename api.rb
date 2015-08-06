#!/usr/bin/env ruby

require 'sinatra'
require 'cassandra'

cass_host = ARGV.first || '127.0.0.1'

cassandra_cluster = Cassandra.cluster(
  reconnection_policy: Cassandra::Reconnection::Policies::Constant.new(5),
  hosts: [ cass_host ],
)
cass = cassandra_cluster.connect('ngmon');

post '/batch_update' do
  # content is in this format:
  #[
  #  {
  #    'update_type' => sometype,
  #    'stamp'       => stamp bigint,
  #    'node'        => node,
  #    'data'        => thedata,
  #  },
  #  {
  #    'update_type' => sometype,
  #    'stamp'       => stamp bigint,
  #    'node'        => node,
  #    'data'        => thedata,
  #  },
  #  [...]
  #]
  puts "got a req"

  #FIXME: check syntax
  reqs = JSON.parse(request.body.read)

  #FIXME: bufferize probe_history in bigger requests.

  reqs.each do |req|

    require 'pp'
    pp req

    # we can receive only two types of batch updates
    # a fullstate and a probe.
    if req['type'] == 'probe'
      # probe processing
      # we only record it for historization purpose.
      # FIXME: batch this.
      statement = cass.prepare("INSERT INTO probe_history (node,probe,stamp,rawdata) VALUES(?,?,?,?)")
      cass.execute(statement, arguments: [req['node'],req['data']['probe_name'],req['stamp'],JSON.generate(req['data'])])
    elsif req['type'] == 'fullstate'
      statement = cass.prepare("INSERT INTO state (node,probe,stamp,rollstate,state,oldstate,next_run,last_output) VALUES(?,?,?,?,?,?,?,?)");
      req['data'].each do |probe_name,pdata|
        # FIXME: delete non existent states from here too.

        cass.execute(statement, arguments: [req['node'],probe_name,req['stamp'],pdata['rollstate'],pdata['state'],pdata['oldstate'],pdata['next_run'],pdata['last_output']])
      end


      # ALSO do state_history with this!

    else
      # bad line!!!
      puts "bad line"
      require 'pp'
      pp req
    end
  end

  'OK'
end
