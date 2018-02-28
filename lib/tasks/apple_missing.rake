require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

namespace :apple do

  namespace :missing do

    desc 'Appends missing duns'
    task append: :environment do

      DnB::Direct::Plus.use_credentials 'lssJnyFNpqud7iX92IjUJuBAGR4DNptk', 'MpXD2l66jwbW3QCS'
      ctr = 0

      CSV.open(File.join("/Users/jihaia/Temp/missing_duns", "exceptions.csv"), "w") do |exceptions|
        CSV.foreach(File.join("/Users/jihaia/Desktop", "missing_duns.csv"), headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
          ctr += 1
          next unless ctr >= 18200

          row_duns = row[:duns]

          unless row_duns.nil?
            duns = row_duns.rjust(9, '0')
            resp = DnB::Direct::Plus::Content.plus_executives duns: duns
            org = resp["organization"]
            if org
              File.open(File.join("/Users/jihaia/Temp/missing_duns", "#{org["duns"]}.json"), "wb") do |ghf|
                formatted = {"organization" => org}
                ghf.puts formatted.to_json
              end
            else
              exceptions << [duns, resp["error"]["errorCode"], resp["error"]["errorMessage"]]
            end

          end

          if ctr % 100 == 0
            p "Processed #{ctr} rows"
            exceptions.flush
          end
        end
      end
    end
  end
end
