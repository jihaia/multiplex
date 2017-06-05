require 'csv'

namespace :pure do

  desc 'Prepares log file to csv'
  task prepare: :environment do
    ctr = 0
    File.open(File.join("/Volumes/D&B Passport/purestorage", "ip access logs", "processed.txt"), "w") do |out|
      ['0426', '0427', '0428', '0429', '0430', '0501', '0502', '0503', '0504', '0505', '0506', '0507', '0508', '0509'].each do |file_num|
        File.foreach(File.join("/Volumes/D&B Passport/purestorage", "ip access logs/all", "access_log-2017#{file_num}")) do |line|
          parts = line.split(' ')
          ctr += 1
          out.puts parts[1] if parts[1] != '-'

          p "Processed #{ctr} rows" if ctr % 10000 == 0

        end
      end
    end
  end

  desc 'Process prepared file'
  task pp: :environment do
    uips = []
    ctr = 0
    p "Processing prepared file"
    File.open(File.join("/Volumes/D&B Passport/purestorage", "ip access logs", "reduced.txt"), "w") do |out|
      File.foreach(File.join("/Volumes/D&B Passport/purestorage", "ip access logs", "processed.txt")) do |line|
        unless uips.include?(line)
          out << line
          uips << line
        end
        ctr += 1
        if ctr % 10000 == 0
          p "Processed #{ctr} rows"
          p "  -- Found #{uips.size} so far"
        end
      end
    end

    p "Found #{uips.size} unique ips"

  end

  desc 'Report matches'
  task report: :environment do
    p "Reporting total matches"
    total = 0
    found = 0
    errors = 0
    CSV.foreach("/Volumes/D&B Passport/purestorage/ip access logs/processed-matched-alt.csv", encoding: 'ISO-8859-1') do |line|

total += 1
      begin
      vals = line.compact || []
      found += 1 if vals.count >1
    rescue => ex
      errors += 1
    end

    end

    p "Found #{found} in #{total} with #{errors} errors"

  end

  desc 'Process first 500'
  task process: :environment do

    DnB::Direct::Plus.use_credentials 'AUvFfV4HmnM1kaGyUoRCMgA99DmtGE6n', 'GYPkpLB5SwsdNEVg'
    ctr = 0

    CSV.open(File.join("/Volumes/D&B Passport/purestorage", "ip access logs", "processed-and-matched.csv"), "w") do |csv|
      File.foreach(File.join("/Volumes/D&B Passport/purestorage", "ip access logs", "processed-unique.txt")) do |line|
        ip_address = line.gsub("\n", '')
        ctr += 1
        resp = DnB::Direct::Plus::Search.ip ip_address
        if resp["inquiryMatch"].nil?
          csv << [ip_address]
        else
          r = resp["inquiryMatch"]
          o = resp["organization"]
          csv << [r["ipAddress"], r["duns"], r["ipDomainName"], o["name"], r["countryISOAlpha2Code"], r["region"]]
        end
        if ctr % 500 == 0
          p "Processed #{ctr} rows"
          csv.flush
        end
      end
    end

    p "Processed #{ctr} in total"
  end # task
end # namespace
