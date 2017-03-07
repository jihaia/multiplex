connection = Bunny.new
connection.start

consumer = nil

ch = connection.create_channel
q = ch.queue("testq")

x = ch.default_exchange

consumers = []

(1..5).each do |idx|
id = "consumer_#{idx}"
consumer = Multiplex::MatchConsumer.new(ch, q, id)
q.subscribe_with(consumer)
consumers << consumer
#consumer.start!
end

start = Time.now
(1..100).each {|y| x.publish("HELLO #{y}", :routing_key => "testq")}
p "PUBLISHED in #{Time.now - start}"
