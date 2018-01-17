require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

namespace :apple do

  namespace :ent_us do

    desc 'Rematch the 13K'
    task rematch: :environment do
      ctr = 0
      started_at = Time.now
      DnB::Direct::Plus.use_credentials 'lssJnyFNpqud7iX92IjUJuBAGR4DNptk', 'MpXD2l66jwbW3QCS'
      target_path = File.join(Rails.root, "tmp", "us_ent_rematch")

      CSV.open(File.join(Rails.root, "tmp", "us_ent_rematch_exceptions.csv"), "w") do |exceptions|
        CSV.foreach(File.join(Rails.root, "tmp", "us_ent_rematch.csv"), headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
        duns = row[:matchcandidates0organizationduns]
        pd = "%09d" % duns
        res = DnB::Direct::Plus::Content.plus_executives(duns: pd)
        if res["organization"].nil?
          exceptions << [
            pd,
            res["error"]["errorCode"],
            res["error"]["errorMessage"]
          ]
          exceptions.flush
        else
          out = { organization: res["organization"] }
          File.open(File.join(target_path, "#{pd}.json"), 'w') {|f| f.write(out.to_json) }
        end

        ctr += 1
        step = 100
        if ctr % step == 0
          diff = Time.now - started_at
          p "Processed #{step} rows in #{diff}[ms] - Total Processed: #{ctr}"
          started_at = Time.now
        end
      end
      end
      p "Total rows processed were #{ctr}"
    end

    desc 'Matches file to db'
    task count: :environment do
      ctr = 0

        CSV.foreach(File.join(Rails.root, "tmp", "sfdc_trans.csv"), headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
            ctr += 1
        end
        p "Total rows #{ctr}"
    end

    desc 'Matches file to db'
    task match: :environment do
      ctr = 0
      started_at = Time.now

      CSV.open(File.join("/Volumes/D&B Passport/apple/ent/source", "101K Output File.csv"), "wb") do |csv|
        CSV.foreach(File.join("/Volumes/D&B Passport/apple/ent/source", "101K Input File.csv"), headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|

            entry = row.to_hash
            data = {
              id: entry[:company_id],
              name: entry[:name],
              address: entry[:address],
              state: entry[:state],
              city: entry[:city],
              zip: entry[:zip],
              country: entry[:country],
              phone: entry[:phone],
              email: entry[:email],
              source: entry[:source],
              apple_identifier: entry[:apple_identifier],
              website: entry[:website]
            }

            # Construct arguments only with those with values
            args = {
              country_code: 'US'
            }

            args.merge!({name: data[:name]}) unless data[:name].nil?
            args.merge!({street_address: data[:address]}) unless data[:address].nil?
            args.merge!({city: data[:city]}) unless data[:city].nil?
            args.merge!({state: data[:state]}) unless data[:state].nil?
            args.merge!({postal_code: data[:zip]}) unless data[:zip].nil?
            args.merge!({telephone: data[:phone]}) unless data[:phone].nil?
            args.merge!({email: data[:email]}) unless data[:email].nil?
            args.merge!({url: data[:website]}) unless data[:website].nil?


            resp = DnB::Direct::Plus::Match.identity_resolution(args)

            candidate = resp.matchCandidates.first

            unless candidate.nil?
              mqi = candidate.matchQualityInformation.symbolize_keys!
              org = candidate.organization

              data.merge!({primaryName: org.primaryName, duns: org.duns, confidenceCode: mqi[:confidenceCode], matchGrade: mqi[:matchGrade]})
            end

            ctr += 1

            csv << [
              ctr,
              data[:duns],
              data[:primaryName],
              data[:confidenceCode],
              data[:matchGrade],
              data[:id],
              data[:name],
              data[:address],
              data[:state],
              data[:city],
              data[:zip],
              data[:country],
              data[:phone],
              data[:email],
              data[:source],
              data[:apple_identifier],
              data[:website]
            ]

            step = 100
            if ctr % step == 0
              diff = Time.now - started_at
              p "Processed #{step} rows in #{diff}[ms] - Total Processed: #{ctr}"
              started_at = Time.now
              csv.flush
            end

        end # csv.foreach
      end
    end # match

  end # ett_sfdc

  namespace :ett_mdm do

    task extract: :environment do
      ctr=0
      File.open(File.join("/Volumes/D&B Passport/apple", "ett", "apple_ett_mdm_extract.txt"), "w") do |out|
        File.foreach(File.join("/Volumes/D&B Passport/apple", "ett", "apple_ett_mdm.txt")) do |csv_line|
          out.puts csv_line
          ctr += 1
          break if ctr % 10000 == 0
        end
      end
    end

    task count: :environment do
      ctr=0
        File.foreach(File.join("/Volumes/D&B Passport/apple", "ett", "apple_ett_mdm.txt")) do |csv_line|
          ctr += 1
        end
      p "Found #{ctr} rows."
    end

    task chunk: :environment do

      row_ctr = 0
      header = []
      file_ctr = 1
      out_file_name = "apple_ett_mdm_mm_"
      file_in_name = "apple_ett_mdm.txt"
      file_out_path = "prep"

      File.foreach(File.join("/Volumes/D&B Passport/apple", "ett", file_in_name)) do |csv_line|


        items = csv_line.split('^')
        i = items.map{|i| i.gsub(/\"$/, '').gsub(/^\"/, "").gsub(/"/,"'")}

        row = CSV.parse(i.join('^'), col_sep: '^').first

        if header.empty?
          header = row.map(&:to_sym)
          next
        end

        entry = Hash[header.zip(row)]
        row_ctr += 1

        # concatenate source system and primary key for further tracking
        src_key = "#{entry[:SRC_SYSTEM_CD]}|#{entry[:PKEY_SRC_OBJECT]}"

        query_params = {candidateMaximumQuantity: 1}

        # Legal Name
        unless entry[:LEGAL_NM].blank?
          query_params.merge!(name: entry[:LEGAL_NM].url_encode)
        end

        # DBA Name
        unless entry[:DBA_NM].blank?
          query_params.merge!(name: entry[:DBA_NM].url_encode)
        end

        # Street Address Line 1
        unless entry[:ADDR1_TXT].blank?
          query_params.merge!(streetAddressLine1: entry[:ADDR1_TXT].url_encode)
        end

        # City
        unless entry[:CITY_NM].blank?
          query_params.merge!(addressLocality: entry[:CITY_NM].url_encode)
        end

        # State / Province
        unless entry[:STATE_PROVINCE_CD].blank?
          query_params.merge!(addressRegion: entry[:STATE_PROVINCE_CD].url_encode)
        end

        # Postal Code
        unless entry[:POSTAL_CD].blank?
          query_params.merge!(postalCode: entry[:POSTAL_CD].url_encode)
        end

        # Telephone Number
        unless entry[:PHONE_NUM].blank?
          query_params.merge!(telephoneNumber: entry[:PHONE_NUM].url_encode)
        end

        if entry[:COUNTRY_ISO2_CD].blank?
          query_params.merge!(countryISOAlpha2Code: "US")
        else
          query_params.merge!(countryISOAlpha2Code: entry[:COUNTRY_ISO2_CD].url_encode)
        end



        params = []
        query_params.each { |k,v| params << "#{k}=#{v}" }

        query_string = params.join('&')

        if row_ctr % 5000 == 0
          p "Processed #{row_ctr} rows"
        end

        if row_ctr % 125000 == 0
          file_ctr += 1
        end

        CSV.open(File.join("/Volumes/D&B Passport/apple", "ett", file_out_path,  "#{out_file_name}#{file_ctr}.csv"), "ab") do |csv|
          csv << [src_key, query_string]
        end
      end
    end


        task repair: :environment do

          row_ctr = 0
          header = []

          CSV.open(File.join("/Volumes/D&B Passport/apple", "ett", "apple_ett_mdm_repaired.txt"), "w") do |out|

          File.foreach(File.join("/Volumes/D&B Passport/apple", "ett", "apple_ett_mdm.txt")) do |csv_line|

            items = csv_line.split('^')
            i = items.map{|i| i.gsub(/\"$/, '').gsub(/^\"/, "").gsub(/"/,"'")}

            row = CSV.parse(i.join('^'), col_sep: '^').first

            if header.empty?
              header = row.map(&:to_sym)
              next
            end

            entry = Hash[header.zip(row)]
            row_ctr += 1

            if row_ctr == 1
              out << entry.keys
            else
              out << entry.values
            end


            if row_ctr % 5000 == 0
              p "Processed #{row_ctr} rows"
            end

          end

          end
        end


  end
end # apple
