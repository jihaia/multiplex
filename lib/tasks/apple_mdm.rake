require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

namespace :apple do

  namespace :mdm do

<<<<<<< HEAD
    desc 'Loads seeds'
    task load_seeds: :environment do
      ctr = 0

      ground_zero = Time.now
      loop = Time.now

        f1 = 'APPLE_MDM_cmpelk_v2_PROD_4_20171129184018_SEEDFILE_1.txt'
        f2 = 'APPLE_MDM_cmpelk_v2_PROD_CHINA_20171129015121_SEEDFILE_1.txt'
        f3 = 'APPLE_MDM_cmpelk_v2_PROD_20171121203557_SEEDFILE_1.txt'

        # filename = f3

        #('a'..'p').each do |ext|
          filename = f2
          p "Processing file #{filename}"
          conn = ActiveRecord::Base.connection
          File.open(File.join('/Volumes/D&B Passport/apple/mdm/seeds', filename)).each do |line|
            json = JSON.parse(line).deep_symbolize_keys
            org = json[:organization]

            duns = org[:duns]
            name = (org[:primaryName].nil? ? 0 : 1)

            # primary address
            pa = org[:primaryAddress]
            al1 = 0
            unless pa[:streetAddress].nil?
                al1 = 1 unless pa[:streetAddress][:line1].nil?
            end

            acity = 0
            unless pa[:addressLocality].nil?
                acity = 1 unless pa[:addressLocality][:name].nil?
            end

            as = 0
            unless pa[:addressRegion].nil?
              as = 1 unless pa[:addressRegion][:name].nil?
            end

            az = (pa[:postalCode].nil? ? 0 : 1)
            ac = (pa[:addressCountry].nil? ? 0 : 1)

            hi = 0
            unless org[:corporateLinkage].nil?
              hi = 1 unless org[:corporateLinkage][:hierarchyLevel].nil?
            end

            dic = 0
            sic = 0
            nic = 0
            # extract industry codes
            unless org[:industryCodes].nil?
              org[:industryCodes].each do |ic|
                # p ic
                # sic
                if(ic[:priority] == 1 && ic[:typeDnBCode] == 3599)
                  sic = 1
                end
                #
                # nic -
                if(ic[:priority] == 1 && ic[:typeDnBCode] == 30832)
                  nic = 1
                end

                if(ic[:priority] == 1 && ic[:typeDnBCode] == 24657)
                  dic = 1
                end
              end
            end

            gr = 0
            unless org[:financials].nil?
              org[:financials].each do |fin|
                fin[:yearlyRevenue].each do |yr|
                  gr = 1 if yr[:currency] == "USD"
                end
              end
            end

            ge = 0
            unless org[:numberOfEmployees].nil?
              org[:numberOfEmployees].each do |emp|
                if emp[:informationScopeDescription] == 'Consolidated'
                  ge = 1
                end
              end
            end

            lat = (pa[:latitude].nil? ? 0 : 1)
            lon = (pa[:longitude].nil? ? 0 : 1)

            m = 0
            unless org[:dunsControlStatus].nil?
              m = org[:dunsControlStatus][:isMarketable] ? 1 : 0
            end

            d = 0
            unless org[:websiteAddress].nil?
              unless org[:websiteAddress].first.nil?
                d = 1
              end
            end

            n = 0
            unless org[:registrationNumbers].nil?
              unless org[:registrationNumbers].first.nil?
                n = 1
              end
            end

            sql = "INSERT INTO regional_stats (" +
                  "duns, " +
                  "company_name, " +
                  "address_line_1, " +
                  "address_city, " +
                  "address_state, " +
                  "address_zip, " +
                  "address_country, " +
                  "hierarchy, " +
                  "dnb_industry_code, " +
                  "sic_industry_code, " +
                  "naics_industry_code, " +
                  "global_revenue, " +
                  "global_employees, " +
                  "geo_lat, " +
                  "geo_lon, " +
                  "marketable, " +
                  "domain, " +
                  "national_id) VALUES (" +

                  # COMPANY_NAME
                  "#{duns}, " +
                  "#{name}, " +
                  "#{al1}, " +
                  "#{acity}, " +
                  "#{as}, " +
                  "#{az}, " +
                  "#{ac}, " +
                  "#{hi}, " +
                  "#{dic}, " +
                  "#{sic}, " +
                  "#{nic}, " +
                  "#{gr}, " +
                  "#{ge}, " +
                  "#{lat}, " +
                  "#{lon}, " +
                  "#{m}, " +
                  "#{d}, " +
                  "#{n}" +
                  ")"

            ctr += 1

            ActiveRecord::Base.connection.execute(sql)

            if ctr % 1000 == 0
              diff = Time.now - loop
              p "Processed #{ctr} rows in #{diff}[s]"
              loop = Time.now
            end

            # break if ctr == 201
          end
        #end
        diff = Time.now - ground_zero
        p "Finished #{ctr} in #{diff}[s]"
    end # match

    desc 'Repairs seed countries'
    task repair_country: :environment do
      ctr = 0

      ground_zero = Time.now
      loop = Time.now

        f1 = 'APPLE_MDM_cmpelk_v2_PROD_4_20171129184018_SEEDFILE_1.txt'
        f2 = 'APPLE_MDM_cmpelk_v2_PROD_CHINA_20171129015121_SEEDFILE_1.txt'
        f3 = 'APPLE_MDM_cmpelk_v2_PROD_20171121203557_SEEDFILE_1.txt'

        # filename = f3

        ('a'..'p').each do |ext|
          filename = "xa#{ext}"
          p "Processing file #{filename}"
          conn = ActiveRecord::Base.connection

          File.open(File.join('/Volumes/D&B Passport/apple/mdm/seeds', filename)).each do |line|
            json = JSON.parse(line).deep_symbolize_keys
            org = json[:organization]

            duns = org[:duns]
            pa = org[:primaryAddress]
            ac = pa[:addressCountry].nil? ? "null" : "'#{pa[:addressCountry][:isoAlpha2Code]}'"

            sql = "UPDATE regional_stats SET " +
                  "address_country = #{ac} " +
                  "WHERE duns = '#{duns}'"


            ctr += 1
            ActiveRecord::Base.connection.execute(sql)

            if ctr % 1000 == 0
              diff = Time.now - loop
              p "Processed #{ctr} rows in #{diff}[s]"
              loop = Time.now
            end

            # break if ctr == 201
          end
        end
        diff = Time.now - ground_zero
        p "Finished #{ctr} in #{diff}[s]"
    end # match

=======
    desc 'Process MB1'
    task mb1: :environment do
      ctr = 0
      started_at = Time.now
      abs_start = Time.now

        mb_num = 2

        (1..1).each do |idx|
          val = idx.to_s.rjust(2, '0')
          file_name = File.join("/Volumes/D&B Passport/apple/mdm-project/matched/ame/AME-MB2-CONSOLIDATED-CSV", "JP-#{val}.csv")
            p file_name
          CSV.foreach(file_name, headers: true, header_converters: :symbol, encoding: 'UTF-8') do |row|

              entry = row.to_hash
              # p entry
              sql = "update ame_crosswalks set confidence_code = #{entry[:confidence_code]}, match_grade = '#{entry[:match_grade]}', duns_number = '#{entry[:match_duns]}' where pk = '#{mb_num}-#{entry[:lookup_number]}';"
              # p sql
              ActiveRecord::Base.connection.execute(sql)

              step = 100
              ctr += 1
              if ctr % step == 0
                diff = Time.now - started_at
                p "Processed #{step} rows in #{diff}[sec] - Total Processed: #{ctr}"
                started_at = Time.now
              end

              # break

          end # csv.foreach

        end

        diff = Time.now - abs_start
        p "Finalized run of #{ctr} rows in #{diff}[sec]"
    end # mb1
>>>>>>> 92d466102e6bfbbf78a32ba5c6e4e536d1303dd0

  end # mdm

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
