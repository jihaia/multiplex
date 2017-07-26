require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

def ame? country
  ["JP","CN", "KO"].include?(country)
end

namespace :apple do

    desc 'Creates bulk file for multi process'
    task bulk: :environment do

      row_ctr = 0
      header = []
      file_in = File.join("/Volumes/D&B Passport/apple", "bulk", "DNB_APPLE_SAMPLE_ISO2.csv")
      file_out = File.join("/Volumes/D&B Passport/apple", "bulk", "prepapred.csv")


      #File.foreach(File.join("/Volumes/D&B Passport/apple", "ett", file_in_name)) do |csv_line|
      CSV.foreach(file_in, headers: true, header_converters: :symbol, encoding: 'UTF-8') do |entry|
        row_ctr += 1

        # concatenate source system and primary key for further tracking
        src_key = entry[:source_id]

        query_params = {candidateMaximumQuantity: 1}

        country_cd = entry[:country_cd]

        if country_cd.blank?
          query_params.merge!(countryISOAlpha2Code: "US")
        else
          query_params.merge!(countryISOAlpha2Code: country_cd.url_encode)
          in_language = case country_cd
          when "JP"
            "ja-JP"
          when "CN"
            "zh-hans-CN"
          when "KR"
            "ko-hang-KR"
          else
            "en-US"
          end

          query_params.merge!(inLanguage: in_language)
        end

        # Legal Name
        unless entry[:company_name].blank?
          if ame? country_cd
            query_params.merge!(name: entry[:company_name])
          else
            query_params.merge!(name: entry[:company_name].url_encode)
          end
        end

        # Street Address Line 1
        unless entry[:address1].blank?
          if ame? country_cd
            query_params.merge!(streetAddressLine1: entry[:address1])
          else
            query_params.merge!(streetAddressLine1: entry[:address1].url_encode)
          end
        end

        # Street Address Line 2
        unless entry[:address2].blank?
          if ame? country_cd
            query_params.merge!(streetAddressLine2: entry[:address2])
          else
            query_params.merge!(streetAddressLine2: entry[:address2].url_encode)
          end
        end

        # city
        city = entry[:city]
        unless city.blank? || ['none', '-', '--'].include?(city)
          if ame? country_cd
            query_params.merge!(addressLocality: entry[:city])
          else
            query_params.merge!(addressLocality: entry[:city].url_encode)
          end
        end

        # State / Province
        unless entry[:state].blank?
          query_params.merge!(addressRegion: entry[:state].url_encode)
        end

        # Postal Code
        unless entry[:postal_cd].blank?
          query_params.merge!(postalCode: entry[:postal_cd].url_encode)
        end

        params = []
        query_params.each { |k,v| params << "#{k}=#{v}" }

        query_string = params.join('&')

        if row_ctr % 5000 == 0
          p "Processed #{row_ctr} rows"
        end

# p query_string

        File.open(file_out, "w") do |file|
          file.write src_key + "," + query_string
        end

        break if row_ctr == 5001
      end
    end


end # apple
