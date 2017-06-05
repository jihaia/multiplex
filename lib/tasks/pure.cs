ctr = 0
uips = [] of String

p "Processing via Crystal"

File.open("/Volumes/D&B Passport/purestorage/ip access logs/processed-out.txt", "w") do |out|
  File.each_line("/Volumes/D&B Passport/purestorage/ip access logs/processed copy.txt") do |line|
    ctr += 1
    unless uips.includes?(line)
      uips << line
      out.puts line
    end

    if ctr % 10000 == 0
      p "Processed #{ctr}"
      out.flush
    end
  end
end

puts ctr
