require 'csv'

class Converter
  attr_accessor :file
  attr_accessor :keys
  attr_accessor :key_name
  attr_accessor :row

  attr_accessor :headers

  def initialize(file_in, file_out)
    @line_count = 0
    @file = file_in
    @output = file_out
    @keys = []
    @headers = []
  end

  # Extract just the keys across all files first, then take a second pass and pull
  # the data.
  def execute(options={})
    build_headers
    write_data(@output)
  end

  def build_headers
    p "[INFO] Started JSON to CSV Flattening"
    p "[INFO] Gathering structure from source file"
    File.open(@file).each do |line|
      @line_count += 1
      next if line.blank?
      obj = JSON.parse(line)
      next if obj["error"]
      keys = (get_keys(obj) || []).uniq
      @headers = (@headers + keys).uniq
    end
    nil

    @headers.sort!

    p "[INFO] Identified #{@headers.size} attribute paths in #{@line_count} rows."
  end

  def already_missing? key, list=[]
    missing = false
    list.each do |entry|
      if key.start_with? entry
        missing = true
        break
      end
    end
    missing
  end

  def write_data file_name
    p "[INFO] Preparing to process file for data"

    bar = RakeProgressbar.new(@line_count)

    # Open the output file to write rows
    CSV.open(file_name, 'w+') do |csv|

      # Start with the headers being written first
      csv << @headers
      ctr = 0

      File.open(@file).each do |line|
        row = {}
        missing = [] # represents the missing keys
        ctr += 1
        bar.inc
        next if line.blank?
        obj = JSON.parse(line)
        next if obj["error"]
        # p @headers
        @headers.each do |key|

          # Purpose is to take a Hash object (parsed from JSON) and fetch the data
          # using the list of registered keys from the headers. This has the potential
          # to be a laborious task given the potential number of keys. That said, we should
          # test each newly supplied key against a list of failed keys to eliminate
          # unneccessary processing.
          #unless already_missing?(key, missing)
            data = get_data(obj, key)
            row.merge!(key => data)
            #data.nil? ?  missing << key : row.merge!(key => data)
          #end
        end

        csv << row.values
        if ctr % 10 == 0
          csv.flush
        end
      end

      bar.finished
    end
  end # def write_data

  def get_root_simple_keys(obj)

    #Tour the root level of the obj hash build the keys array.
    obj.each do |key, value|
      #p "keys: " + @keys.to_s
      @key_name = "" #Here at the root level so initialize.
      @level = "root"
      case value
        when Numeric, String, false, true
          #handle_simple(value)
          @keys << key #Reached end-point.
      end
    end
  end


  def get_keys(obj)
    @keys = []

    # get_root_simple_keys(obj)

    #Tour the root level of the obj hash build the keys array.
    obj.each do |key, value|
      #p "keys: " + @keys.to_s
      @key_name = "" #Here at the root level so initialize.
      @level = "root"
      case value
        when Numeric, String, false, true
          #pass, already picked up in get_root_simple_keys.
        when Hash
          @key_name = key
          handle_hash(value) #go off and handle hashes!
        when Array
          @key_name = key
          handle_array(value) #go off and handle arrays!
        else
          p @key_name
          #p key, value
          # p "WARN: Unexpected type in obj hash: #{value}"
      end
    end

    return @keys
  end

  def is_int?(val)
    result = true
    begin
    Integer(val)
    rescue
      result = false
    end

    result
  end

  def get_data(obj, key)
    lookup = obj
    keys = key.split(".")
    val = nil

    # monitor = (key == 'organization.dunsControlStatus.isTelephoneDisconnected')

    # loop through each facet of the key
    keys.each { |key|
      # p key if monitor
      if key.to_f < 0
        p "ERROR with negative key #{key}"
      end

      begin
        if is_int?(key)
          # p lookup
          lookup = lookup[Integer(key)]
          # p lookup
        else
          lookup = lookup[key]
        end

        # p lookup if monitor
      rescue => e
        # p "[OOPS] #{key} in #{keys} failed" if monitor
      end
    }

    # begin
    #   if key.split(".")[-1] == "id"
    #     lookup = lookup.split(":")[-1]
    #   end
    # rescue
    #   # do nothing, this is a media or user_mention id, with no ":" delimiters.
    # end

    # Remove newline characters on the way out.
    begin
      #if lookup.is_a? String and !lookup.nil?
      #    lookup.gsub!(/\n/, "")
      # end
    rescue Exception => e
      p "ERROR in get_data method, removing new lines: #{e.message}"
    end

    # p "[GET DATA] #{key} :: #{lookup}"

    lookup
  end

  def primitive_array(string)
    if string.scan('{').count == 0 and string.scan('[').count == 1 then
      return true
    end
    return false
  end

  def handle_array(array)

    name_root = @key_name
    #Now examine value to determine its type.

    @level = @level + '.array'

    key = -1

    array.each { |value|

      #p array.to_s

      key = key + 1

      if primitive_array(array.to_s) and key > 0 then
        @key_name = "#{@key_name.split(".")[0..-2].join(".")}.#{key}"

      else
        @key_name = "#{@key_name}.#{key}"
      end

      case value
        when Numeric, String, NilClass, false, true
          @keys << "#{@key_name}" #Done here.

        when Hash
          #@name = "#{@name}.#{key}"
          handle_hash(value) #go off and handle hashes!
          if key == (array.length - 1) then
            @key_name = name_root.split(".")[0..-2].join(".")
          end
        when Array
          #name = "#{name}.#{key}"
          handle_array(value) #go off and handle arrays!
          if key == (array.length - 1) then
            @key_name = name_root.split(".")[0..-2].join(".")
          end
        else
          p "WARNING: Unexpected type in activity array: #{value}"
      end
    }

    @key_name = name_root.split(".")[0..-2].join(".")
    @level = @level.split(".")[0..-2].join(".")
  end

  def handle_hash(hash)

    #p "Handling hash: #{hash.to_s}, arriving with name: #{@key_name}"

    @level = @level + '.hash'

    hash_item = 0

    #Tour this hash determining the value types.
    hash.each { |key, value|

      hash_item = hash_item + 1

      case value

        when Numeric, String, NilClass, false, true
          @keys << "#{@key_name}.#{key}" #Done here.

          #reset key_name back if array?
          begin
            #Float(@key_name.split(".")[-1])
            if hash.length == hash_item then
              @key_name = @key_name.split(".")[0..-2].join(".")
            end
          rescue Exception => e
            p "Error in handle_hash method: #{e.message}"
          end

        when Hash
          next if value.empty?
          @key_name = "#{@key_name}.#{key}"
          handle_hash(value) #go off and handle hashes!
          if hash_item == hash.length then
            @key_name = @key_name.split(".")[0..-2].join(".")
          end
        when Array
          @key_name = "#{@key_name}.#{key}"
          handle_array(value) #go off and handle arrays!
          if hash_item == hash.length then
            @key_name = @key_name.split(".")[0..-2].join(".")
          end
        else
          p "WARNING: Unexpected type in activity hash: #{value}"
      end
    }

    @level = @level.split(".")[0..-2].join(".")
  end

  #With simple 'keys' we just add the key name to the keys array.
  def handle_simple(value)
    @keys << @key_name
  end

end

namespace :apple do

  desc 'Joins the json appends'
  task :join do
    # folder = '/Users/jihaia/Downloads/F1k G500 not loaded-030718-append'
    folder = '/Users/jihaia/Downloads/triage'
    # target = '/Users/jihaia/Downloads/F1k G500 not loaded-030718-append.json'
    target = '/Users/jihaia/Downloads/triage.json'
    ctr = 1

    Dir.glob(File.join(folder, "*.json")) do |filename|

      contents = File.read(filename) + "\n"
      File.open(target, 'a+') {|f| f.write(contents) }
      #break if ctr == 100
      ctr += 1
    end
  end


  desc 'Takes a json file and flattens to csv'
  task json2csv: :environment do
    keys = []
    entries = []
    # file_in = '/Users/jihaia/Downloads/triage/in.json'
    file_in = '/Users/jihaia/Downloads/F1k G500 not loaded-030718-append.json'
    # file_out = '/Users/jihaia/Downloads/triage.csv'
    file_out = '/Users/jihaia/Downloads/F1k G500 not loaded-030718-append.csv'

    Converter.new(file_in, file_out).execute

  end

end # apple
