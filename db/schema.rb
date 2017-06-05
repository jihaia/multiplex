# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(version: 20170512000000) do

  create_table "apple_ett_sfdcs", force: :cascade, options: "ENGINE=InnoDB DEFAULT CHARSET=utf8" do |t|
    t.string  "c_id"
    t.string  "d_customer_id"
    t.string  "ship_to_name"
    t.string  "duns"
    t.string  "primary_name"
    t.string  "confidence_code"
    t.string  "match_grade"
    t.integer "occurences",                                                 default: 1
    t.integer "number_of_employees"
    t.decimal "yearly_revenue",                    precision: 20, scale: 2
    t.text    "trade_style_names",   limit: 65535
    t.text    "industry_codes",      limit: 65535
  end

  create_table "match_results", force: :cascade, options: "ENGINE=InnoDB DEFAULT CHARSET=utf8" do |t|
    t.string "source_id"
    t.string "status"
    t.string "duns"
    t.string "confidence_code"
    t.string "match_grade"
  end

end
