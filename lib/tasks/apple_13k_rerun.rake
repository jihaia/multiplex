require 'csv'

class String
  def url_encode
    self.gsub(/[^a-zA-Z0-9\s]/,'').gsub(/\s+/, '+')
  end
end

namespace :apple do


  desc 'Joins the folder of JSON to a single file'
  task join_json: :environment do

    in_folder = File.join(Rails.root, 'tmp', 'file', '13k_rerun')
    out_file  = File.join(Rails.root, 'tmp', 'file', '13k_rerun.json')

    File.open(out_file, "w") do |out|
      Dir.glob(File.join(in_folder, '*.json')) do |filename|
        out.puts File.read(filename)
      end
    end

  end

    desc 'Reruns append on 13k exceptions'
    task rerun_exceptions: :environment do

      DnB::Direct::Plus.use_credentials 'lssJnyFNpqud7iX92IjUJuBAGR4DNptk', 'MpXD2l66jwbW3QCS'
      ctr = 0

      CSV.open(File.join(Rails.root, "tmp/file/13k_rerun", "exceptions.csv"), "w") do |exceptions|
        CSV.foreach(File.join(Rails.root, "tmp/file", "13K-EXCEPTIONS-APPEND-RERUN.csv"), headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
          ctr += 1
          next unless ctr >= 12600
          row_duns = row[:duns]

          unless row_duns.nil?
            duns = row_duns.rjust(9, '0')
            resp = DnB::Direct::Plus::Content.plus_executives duns: duns
            org = resp["organization"]
            if org
              File.open(File.join(Rails.root, "tmp/file/13k_rerun", "#{org["duns"]}.json"), "wb") do |ghf|
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
