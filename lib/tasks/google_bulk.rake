require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

def ame? country
  ["JP","CN", "KO"].include?(country)
end

def language_cd(country_cd)
  return case country_cd
  when "JP"
    "ja-JP"
  when "CN"
    "zh-hans-CN"
  when "KR"
    "ko-hang-KR"
  else
    "en-US"
  end
end

$PROCESS_AME_ONLY=true
DnB::Direct::Plus.use_credentials 'AUvFfV4HmnM1kaGyUoRCMgA99DmtGE6n', 'GYPkpLB5SwsdNEVg'

namespace :google do

    desc 'Creates bulk file for multi process'
    task bulk: :environment do
      row_ctr = 0
      header = []
      file_in = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3.csv")
      file_out = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3-Matched.csv")
      #ame_out = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3-Prepared-AME.csv")

      # delete the target file
      File.delete(file_out) if File.exists?(file_out)
      #File.delete(ame_out) if File.exists?(ame_out)
CSV.open(file_out, "wb") do |csv|
      CSV.foreach(file_in, headers: true, header_converters: :symbol, encoding: 'UTF-8') do |entry|
        row_ctr += 1

        country_cd = (entry[:billing_country].blank? ? 'US' : entry[:billing_country])
        name = entry[:customer_name]
        street_address = entry[:billing_address_line_1]
        city = entry[:billing_city]
        postal_code = entry[:billing_stateprovince]

        params = {country_code: country_cd, name: name}
        params.merge!(street_address: street_address) unless street_address.blank?
        params.merge!(city: city) unless city.blank?
        params.merge!(postal_code: postal_code) unless postal_code.blank?

        ro = {row: row_ctr}
        resp = DnB::Direct::Plus::Match.identity_resolution(params)

        if resp.matchCandidates.empty?
          p 'No Match'
        else
          matched = resp.matchCandidates.first
          mqi = matched.matchQualityInformation
          org = matched.organization

          ro.merge!(duns: org.duns, name: org.primaryName, confidence_code: mqi["confidenceCode"], match_grade: mqi["matchGrade"])
          unless org.primaryAddress.nil?
            pa = org.primaryAddress
            ro.merge!(
              street_number: pa.streetNumber,
              street_name: pa.streetName,
              locality: pa.addressLocality["name"],
              postal_code: pa.postalCode,
              country_name: pa.addressCountry["name"],
              country_code: pa.addressCountry["isoAlpha2Code"],
              is_registered_address: pa.isRegisteredAddress
              )
          end
        end

        csv << [
          ro[:row],
          ro[:duns],
          ro[:name],
          ro[:confidence_code],
          ro[:match_grade],
          ro[:street_number],
          ro[:street_name],
          ro[:locality],
          ro[:postal_code],
          ro[:country_code],
          ro[:country_name],
          ro[:is_registered_address]]

        if row_ctr % 1000 == 0
          p "Processed #{row_ctr} rows"
        end
        break if row_ctr == 10
      end

      p "[Finished] Processed #{row_ctr} total rows"
end
    end


end # google
