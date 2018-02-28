require 'csv'

namespace :google do
  task domain: :environment do
    file_out = File.join('tmp', 'file', 'google-rfp.csv')
    file_in = File.join('tmp', 'file', 'google-in.csv')

    CSV.open(file_out, "wb") do |csv|
      CSV.foreach(file_in, headers: true, header_converters: :symbol, encoding: 'UTF-8') do |entry|
        p entry
      end
    end

  end
end
