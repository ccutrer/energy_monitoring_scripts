#!/usr/bin/env ruby

require 'bundler/inline'

gemfile do
  gem 'solaredge', '0.1.2'
end

exit if Time.now.hour < 6 || Time.now.hour >= 22

require 'net/http'
require 'yaml'

config = YAML.load_file('tsdb.yml')

se = SolarEdge::Client.new(config['solaredge_api_key'])
site = se.sites.find(config['solaredge_site_id'].to_i)

if config['solaredge_site_time_zone']
  site.instance_variable_set(:@time_zone,
    ActiveSupport::TimeZone.new(config['solaredge_site_time_zone']))
end

post_body = []


backfill = false

startDate = backfill ? site.data_period.begin : site.time_zone.now.beginning_of_day
endDate = startDate + 30.days
while startDate < Time.now
  post_body.concat(site.energy(start_date: startDate, end_date: endDate).map do |point|
    next unless point[:value]
    "energy,circuit=solaredge value=#{-point[:value]} #{point[:timestamp].to_i}"
  end.compact)
  startDate = endDate + 1.day
  endDate = startDate + 30.days
end

startTime = site.time_zone.now.beginning_of_day
startTime -= 7.days if backfill
while startTime < Time.now
  site.inverters.each do |inverter|
    inverter.data(start_time: startTime, end_time: startTime + 1.day).each do |point|
      post_body << "power,circuit=solaredge value=#{-point[:total_active_power]} #{point[:timestamp].to_i}"
      post_body << "voltage,circuit=solaredge,form=dc value=#{point[:dc_voltage]} #{point[:timestamp].to_i}"
      post_body << "temperature,probe=inverter value=#{point[:temperature]} #{point[:timestamp].to_i}"
    end
  end
  startTime += 1.day
end

if backfill
  startTime = site.time_zone.now.beginning_of_day - 31.days
  while startTime < Time.now
    site.power(start_time: startTime, end_time: startTime + 1.day).each do |point|
      next unless point[:value]
      post_body << "power,circuit=solaredge value=#{-point[:value]} #{point[:timestamp].to_i}"
    end
    startTime += 1.day
  end
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
