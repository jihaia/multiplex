require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

namespace :apple do

  namespace :mdm do

    desc 'Process CAN ETT'
    task can_ett: :environment do
      ctr = 0

      ground_zero = Time.now
      loop = Time.now

      DnB::Direct::Plus.use_credentials 'lssJnyFNpqud7iX92IjUJuBAGR4DNptk', 'MpXD2l66jwbW3QCS'
      base_path = "/Users/jihaia/Sandbox/dnb/apple/can ett"

      CSV.open(File.join(base_path, "crosswalk.csv"), "wb") do |crosswalk|
        # write crosswalk header
        crosswalk << [
          'APPLE_IDENTIFIER',
          'DUNS',
          'CONFIDENCE_CODE',
          'MATCH_GRADE'
        ]

        ('b'..'s').each do |ext|
          filename = "#{base_path}/cana#{ext}"
          p "Processing file #{filename}"
          cols = [:company_id, :name, :address, :state, :city, :zip, :phone, :email, :website, :source, :country, :apple_identifier]

          File.open(filename, encoding: 'ISO-8859-1').each do |line|
            # prepare the line to a hash for easy access
            ctr += 1
            items = line.gsub!("\r\n", "").split("\t")
            vals = {}
            cols.each_with_index do |v, i|
              val = (items[i] == '""' ? nil : items[i])
              vals.merge!(v => val)
            end

            # prepare extended match parameters
            query_params = {
              candidate_maximum_quantity: 1,
              min_confidence_code: 7,
              customer_reference: vals[:apple_identifier],
              product_id: 'cmpelk',
              version_id: 'v2'
            }

            country_cd = vals[:country] || 'CA'
            query_params.merge!(name: vals[:name]) unless vals[:name].blank?
            query_params.merge!(street_address: vals[:address]) unless vals[:address].blank?
            query_params.merge!(city: vals[:city]) unless vals[:city].blank?
            query_params.merge!(state: vals[:state].url_encode) unless vals[:state].nil?
            query_params.merge!(country_code: country_cd)
            query_params.merge!(postal_code: vals[:zip]) unless vals[:zip].nil?

            # execute extended match
            resp = DnB::Direct::Plus::Match.extended_match(query_params)
            payload = JSON.parse(resp) || {}

            duns = nil
            confidence_code = nil
            match_grade = nil

            unless payload["matchCandidates"].nil?
              mc = payload["matchCandidates"][0]

              unless mc.nil?
                # parse extended match results
                org = mc["organization"]
                mqi = mc["matchQualityInformation"]

                duns = org["duns"]
                confidence_code = mqi["confidenceCode"]
                match_grade = mqi["matchGrade"]

                # write the append file
                File.open(File.join(base_path, "appends", "#{org["duns"]}.json"), "wb") do |ghf|
                  formatted = {"organization" => org}
                  ghf.puts formatted.to_s
                end

              end
            end

            # write to crosswalk
            crosswalk << [
              vals[:apple_identifier],
              duns,
              confidence_code,
              match_grade
            ]

            if ctr % 100 == 0
              diff = Time.now - loop
              p "Processed #{ctr} rows in #{diff}[s]"
              loop = Time.now
            end # if

          end # File.open

        end # each file
      end # corsswalk

        diff = Time.now - ground_zero
        p "Finished #{ctr} in #{diff}[s]"

    end # can_ett


  end # mdm

end # apple
