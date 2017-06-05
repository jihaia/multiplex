require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

namespace :apple do

  namespace :ett_sfdc do

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

        CSV.foreach(File.join(Rails.root, "tmp", "sfdc_trans.csv"), headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|

            entry = row.to_hash

            d_customer_id = entry[:___d_customer_id]
            c_id = entry[:___c_id]
            name = entry[:shipto_name]
            address = entry[:shipto_address]
            city = entry[:shipto_city]

            occurence = AppleEttSfdc.where(d_customer_id: d_customer_id).first

            if occurence.nil?

              resp = DnB::Direct::Plus::Match.identity_resolution(
                country_code: 'US',
                name: name,
                street_address: address,
                city: city
              )

              candidate = resp.matchCandidates.first
              unless candidate.nil?
                mqi = candidate.matchQualityInformation
                org = candidate.organization

                e_org = DnB::Direct::Plus::Content.profile_with_linkage(duns: org.duns)
                unless e_org.nil?

                  payload = JSON.parse(e_org.payload)
                  entity_type = payload["organization"]["businessEntityType"]["description"]
                  financials = payload["organization"]["financials"].first || {}

                  numOfEmps = payload["organization"]["numberOfEmployees"].first || {}
                  yearlyRev = (financials["yearlyRevenue"].nil? ? {} : financials["yearlyRevenue"].first || {})

                  trade_style_names = []
                  payload["organization"]["tradeStyleNames"].each do |tsn|
                    trade_style_names << tsn["name"]
                  end
                  industry_codes = []
                  payload["organization"]["industryCodes"].each do |ic|
                    industry_codes << ic["description"]
                  end

                  AppleEttSfdc.create(
                    d_customer_id: d_customer_id,
                    c_id: c_id,
                    ship_to_name: name,
                    primary_name: org.primaryName,
                    duns: org.duns,
                    confidence_code: mqi["confidenceCode"],
                    match_grade: mqi["matchGrade"],
                    number_of_employees: numOfEmps["value"],
                    yearly_revenue: yearlyRev["value"],
                    industry_codes: industry_codes.to_s,
                    trade_style_names: trade_style_names
                  )
                end

              end
            else
              inc = occurence.occurences + 1
              occurence.update_attribute(:occurences, inc)
            end
            ctr += 1

            if ctr % 100 == 0
              p "Processed #{ctr} rows"
            end

        end # csv.foreach

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
