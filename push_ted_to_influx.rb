#!/usr/bin/env ruby

require 'bundler/inline'

gemfile do
  gem 'the_energy_detective', '0.1.1'
end

require 'net/http'
require 'yaml'

config = YAML.load_file('tsdb.yml')

ecc = TED::ECC.new(config['ted_uri'])

post_body = []

def history_for(object)
  start = Time.now.utc - 15 * 60

  circuit = object.description.downcase.gsub(/[^\w]/, '_')
  return [] if circuit == 'basement'

  power = object.history(start_time: start).map do |point|
    next if point[:power].abs > 30000 # wtf, TED!
    "power,circuit=#{circuit} value=#{point[:power].to_f} #{point[:timestamp].to_i}"
  end

  energy = object.history(interval: :hours, limit: 24).map do |point|
    "energy,circuit=#{circuit} value=#{point[:energy].to_f} #{point[:timestamp].to_i}"
  end

  power + energy
end

ecc.mtus.each do |mtu|
  post_body.concat(history_for(mtu))
end

ecc.spyders.each do |spyder|
  post_body.concat(history_for(spyder))
end

influx_uri = URI.parse(config['influx_uri'])
http = Net::HTTP.new(influx_uri.host, influx_uri.port)
http.use_ssl = (influx_uri.scheme == 'https')
http.verify_mode = OpenSSL::SSL::VERIFY_NONE
while !post_body.empty?
  slice = post_body.shift(1000)
  post = Net::HTTP::Post.new("/write?db=#{config['db']}&precision=s")
  post.body = slice.join("\n")
  if config['user'] && config['password']
    post.basic_auth config['user'], config['password']
  end
  response = http.request(post)
  puts response.code
  puts response.body if response.body
end
