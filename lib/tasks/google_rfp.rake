require 'csv'

@ctr = 0

def process_item(queue_item)
  begin

  rescue => ex
    @queue.push(queue_item)
  end
end

namespace :google do

  desc 'Sample queuing'
  task queue: :environment do
    queue = Queue.new
    DnB::Direct::Plus.use_credentials 'lssJnyFNpqud7iX92IjUJuBAGR4DNptk', 'MpXD2l66jwbW3QCS'

    # Load queue
    p "Loading source file to queue"
    total = 0
    CSV.foreach('/Users/jihaia/Downloads/Canada ETT File Cleansed for Match.txt', headers: true, header_converters: :symbol, encoding: 'ISO-8859-1', col_sep: "\t").each do |row|
      queue.push row.to_hash
      total += 1
    end

    p "Loaded #{total} rows"

    # Initialize the threads
    p "Initializing five(5) threads"
    threads = Array.new(5) do |x|
      Thread.new do
        until queue.empty?
          next_object = queue.shift
          process_item(next_object)
        end
      end
    end

    begin
      threads.each(&:join)
      p @ctr
    ensure
      # cleanup()
    end

  end

  desc 'Processes by domain'
  task domain: :environment do
    file_out = File.join('tmp', 'file', 'google-rfp.csv')
    file_in = File.join('tmp', 'file', 'google-in.csv')

    CSV.open(file_out, "wb") do |csv|
      CSV.foreach(file_in, headers: true, header_converters: :symbol, encoding: 'UTF-8') do |entry|
        if (/^[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$/ =~ entry[:inquirydetail_name]) == 0
          csv << [entry[:batch_result_id], entry[:inquirydetail_name]]
        end
      end
      nil
    end
    nil

  end
end
