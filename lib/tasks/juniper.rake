require 'csv'

namespace :juniper do

  desc 'Process first 500'
  task process: :environment do

    DnB::Direct::Plus.use_credentials 'AUvFfV4HmnM1kaGyUoRCMgA99DmtGE6n', 'GYPkpLB5SwsdNEVg'
    ctr = 0

    CSV.open(File.join("/Volumes/D&B Passport/juniper/ip/source", "juniper-processed-february-isp.csv"), "wb") do |csv|

      CSV.foreach("/Volumes/D&B Passport/juniper/ip/source/juniper-february-isp.csv", headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|

          entry = row.to_hash
          ip_address = entry[:ip]

        ctr += 1
        resp = DnB::Direct::Plus::Search.ip ip_address
        if resp["inquiryMatch"].nil?
          csv << [ip_address]
        else
          r = resp["inquiryMatch"]
          o = resp["organization"]
          csv << [r["ipAddress"], r["duns"], r["ipDomainName"], o["name"], r["countryISOAlpha2Code"], r["region"]]
        end
        if ctr % 100 == 0
          p "Processed #{ctr} rows"
          csv.flush
        end

      end
    end

    p "Processed #{ctr} in total"
  end # task
end # namespace
