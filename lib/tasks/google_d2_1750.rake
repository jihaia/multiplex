require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end

  def sanitize
    self.gsub!(/[^0-9a-zA-Z]/, ' ')
  end
end


DnB::Direct::Two.use_credentials 'nathansita@dnb.com', 'dnbdirect123'

  namespace :google do
    namespace :d2_1750 do

      desc 'Creates bulk file for multi process'
      task match: :environment do
        row_ctr = 0
        header = []
        file_in = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3.csv")
        file_out = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3-Matched.txt")

        # delete the target file
        File.delete(file_out) if File.exists?(file_out)

        File.open(file_out, "wb") do |file|
        CSV.foreach(file_in, headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |entry|
          row_ctr += 1

          country_cd = (entry[:billing_country].blank? ? 'US' : entry[:billing_country])
          name = entry[:customer_name]
          street_address_line_1 = (entry[:billing_address_line_1] || "").sanitize
          street_address_line_2 = (entry[:billing_address_line_2] || "").sanitize
          city = (entry[:billing_city] || "").sanitize
          state_province = entry[:billing_stateprovince] || ""
          postal_code = entry[:billing_zippostal_code] || ""

          params = {country_code: country_cd, name: name}
          params.merge!(street_address_line_1: street_address_line_1) unless street_address_line_1.blank?
          params.merge!(street_address_line_2: street_address_line_2) unless street_address_line_2.blank?
          params.merge!(city: city) unless city.blank?
          params.merge!(state_province: state_province) unless state_province.nil?
          params.merge!(postal_code: postal_code) unless postal_code.blank?

          resp = DnB::Direct::Two::Match.identity_resolution(params)

          file.puts resp

          if row_ctr % 50 == 0
            p "Processed #{row_ctr} rows"
          end
          #break if row_ctr == 1
        end

        p "[Finished] Processed #{row_ctr} total rows"
      end
      end

      desc 'Appends the matched gile'
      task append: :environment do
        ctr = 0
        file_in = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3-Matched.txt")
        file_out = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3-Append.txt")

        # delete the target file
        File.delete(file_out) if File.exists?(file_out)

        File.open(file_out, "wb") do |file|
          File.foreach(file_in) do |line|
            ctr += 1

            match = JSON.parse(line)
            candidate = {}
            output = {match: match}
            begin
              mrd = match["GetCleanseMatchResponse"]["GetCleanseMatchResponseDetail"]["MatchResponseDetail"]
              candidate = mrd["MatchCandidate"].first
            rescue => ex
            end

            unless candidate["DUNSNumber"].nil?
              duns = candidate["DUNSNumber"]

              append = DnB::Direct::Two::Content.dcp_prem(duns: duns)
              output.merge!(append: JSON.parse(append))

            end
            file.puts output.to_json
            #break if ctr == 1
            if ctr % 50 == 0
              file.flush
              p "Processed #{ctr} rows"
            end

          end
        end

      end # task append

      desc 'Reports the matched gile'
      task report: :environment do
        ctr = 0
        file_in = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3-Append.txt")
        file_out = File.join("/Volumes/D&B Passport/google", "identity-verification-1750", "MatchFile3-Report.txt")

        # delete the target file
        File.delete(file_out) if File.exists?(file_out)

        CSV.open(file_out, "wb") do |csv|
          File.foreach(file_in) do |line|
            ctr += 1

            payload = JSON.parse(line)

            match = payload["match"]
            append = payload["append"]
            candidate = nil

            result = {}

            # match candidate
            begin
              candidate = match["GetCleanseMatchResponse"]["GetCleanseMatchResponseDetail"]["MatchResponseDetail"]["MatchCandidate"].first
              mqi = candidate["MatchQualityInformation"]

              # Output DUNS and match grades
              result.merge!(duns: candidate["DUNSNumber"], confidence_code: mqi["ConfidenceCodeValue"], match_grade: mqi["MatchGradeText"])
              mqi["MatchGradeComponent"].each do |mgc|
                element = mgc["MatchGradeComponentTypeText"]["$"]
                grade = mgc["MatchGradeComponentRating"]
                result.merge!("mg_#{element.split(' ').join('_').downcase}": grade)
              end

              # Output Primary Name
              result.merge!(primary_name: candidate["OrganizationPrimaryName"]["OrganizationName"]["$"])
            rescue => ex
              p ex.message
            end

            unless append.nil?
              unless append["OrderProductResponse"].nil?
                unless append["OrderProductResponse"]["OrderProductResponseDetail"].nil?
                  details = append["OrderProductResponse"]["OrderProductResponseDetail"]
                  org_details = details["Product"]["Organization"]["OrganizationDetail"]

                  unless org_details["FamilyTreeMemberRole"].nil?
                    org_details["FamilyTreeMemberRole"].each do |role|
                      role_type = role["FamilyTreeMemberRoleText"]["$"]
                      result.merge!("is_#{role_type.split(' ').join('_').downcase}": true)
                    end
                  end

                  # Output Standalone
                  result.merge!(is_standalone: org_details["StandaloneOrganizationIndicator"])
                  result.merge!(start_year: org_details["OrganizationStartYear"])

                  if org_details["ControlOwnershipDate"]
                    result.merge!(control_date: org_details["ControlOwnershipDate"]["$"])
                  end

                  reg_details = details["Product"]["Organization"]["RegisteredDetail"]
                  if reg_details
                    result.merge!(inc_year: reg_details["IncorporationYear"])
                  end
                end
              end
            end

            #p result

            csv << [
              result[:duns],
              result[:confidence_code],
              result[:match_grade],
              result[:mg_name],
              result[:mg_street_number],
              result[:mg_street_name],
              result[:mg_city],
              result[:mg_state],
              result[:mg_postal_code],
              result[:mg_density],
              result[:mg_uniqueness],
              result[:primary_name],
              result[:start_year],
              result[:control_date],
              result[:inc_year],
              result[:is_global_ultimate] || false,
              result[:is_domestic_ultimate] || false,
              result[:is_subsidiary] || false,
              result[:is_headquarters] || false,
              result[:is_standalone]
            ]

            # break if ctr == 5
            if ctr % 50 == 0
              csv.flush
              p "Processed #{ctr} rows"
            end

            #break if ctr == 1

          end
        end

      end # task report

    end # namespace :d2_1750
  end # namespace :google
