require 'bunny'

class Multiplex::MatchConsumer < DnB::Direct::Consumer

  def process(delivery_info, metadata, payload)
    #puts "RECEIVED [#{self.consumer_tag}]", payload
    MatchResult.create(source_id: payload, status: 404)
  end

end
