require 'csv'

namespace :yukon do

  desc 'Initiates the multiprocess job'
  task initiate: :environment do
    DnB::Direct::Plus.use_credentials 'lssJnyFNpqud7iX92IjUJuBAGR4DNptk', 'MpXD2l66jwbW3QCS'
    file_name = 'sample-bem.csv'
    file = File.join(Rails.root.to_s, 'tmp', 'file', file_name)
    DnB::Direct::Plus::MultiProcess.initiate_job file_name: file_name, file: file
  end

end
