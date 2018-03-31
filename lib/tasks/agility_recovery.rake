require 'csv'

@ctr = 0
@too_many = 0
@in_file  = File.join(Rails.root, 'tmp', 'file', 'ar', 'AR-ONLY-PROSPECTS.csv')
@out_file = File.join(Rails.root, 'tmp', 'file', 'ar', 'AR-ONLY-PROSPECTS-CROSSWALK.csv')
@queue = Queue.new
@timer = Time.now

def process_queue_item(thread_id, queue_item)
  begin
    country = queue_item[:country] || ''
    country_code = (country.start_with?('United') ? 'US' : 'CA')

    options = {
      candidate_maximum_quantity: 1,
      country_code: country_code,
      name: queue_item[:company_name]
    }

    options.merge!({city: queue_item[:city]}) if queue_item[:city]
    options.merge!({street_address: queue_item[:address]}) if queue_item[:address]
    options.merge!({state: queue_item[:state]}) if queue_item[:state]
    options.merge!({postal_code: queue_item[:postal_code]}) if queue_item[:postal_code]

    resp = DnB::Direct::Plus::Match.identity_resolution(options)
    out = [queue_item[:account_id]]
    mc = resp.matchCandidates[0]
    unless mc.nil?
      out.push(
        mc.organization.duns,
        mc.matchQualityInformation["confidenceCode"],
        mc.matchQualityInformation["matchGrade"]
      )
    end

    write_out(out)

    @ctr += 1
    if @ctr % 100 == 0
      p "PROCESSED #{@ctr} rows. Last 100 completed in #{(Time.now - @timer).round}[sec]"
      @timer = Time.now
    end
  rescue DnB::Direct::Exception::TpsExceededError
    @too_many += 1
    if @too_many % 100 == 0
      p "ENCOUNTERED #{@too_many} requests"
    end
    # p "sleeping thread #{thread_id}"
    sleep thread_id * 3
    @queue.push(queue_item)
  rescue => ex
    p ex.message
  end
end

def write_out(row)
  CSV.open(@out_file, "a+") { |crosswalk| crosswalk << row }
end

namespace :agility do

  desc 'Matches AR source file'
  task match: :environment do

    ctr = 0
    started_at = Time.now
    interim = Time.now

    p "STARTED at #{started_at}"

    DnB::Direct::Plus.use_credentials 'AUvFfV4HmnM1kaGyUoRCMgA99DmtGE6n', 'GYPkpLB5SwsdNEVg'
    # DnB::Direct::Plus.use_credentials 'lssJnyFNpqud7iX92IjUJuBAGR4DNptk', 'MpXD2l66jwbW3QCS'

    # Load queue
    p "Loading source file to queue"
    total = 0

    CSV.foreach(@in_file, headers: true, header_converters: :symbol, encoding: 'ISO-8859-1').each do |row|
      @queue.push row.to_hash
      total += 1
    end

    p "Added #{total} source rows to the queue"

    # Open the out_file and write the headers
    CSV.open(@out_file, "w") do |crosswalk|
      # write crosswalk header
      crosswalk << [
        'IDENTIFIER',
        'DUNS',
        'CONFIDENCE_CODE',
        'MATCH_GRADE'
      ]
    end

    # Initialize the threads
    p "Initializing 4 worker threads"
    thread_ctr = 0
    threads = Array.new(4) do |x|
      Thread.new do
        id = (thread_ctr +=1)
        until @queue.empty?
          next_object = @queue.shift
          process_queue_item(id, next_object)
        end
      end
    end

    begin
      threads.each(&:join)
    ensure
      p "FINISHED at #{Time.now}"
      p "Total time of #{Time.now - started_at}"
    end
  end

end
