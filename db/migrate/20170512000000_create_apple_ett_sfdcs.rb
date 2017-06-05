class CreateAppleEttSfdcs < ActiveRecord::Migration
  def change
    create_table :apple_ett_sfdcs do |t|
      t.string :c_id
      t.string :d_customer_id
      t.string :ship_to_name
      t.string :duns
      t.string :primary_name
      t.string :confidence_code
      t.string :match_grade
      t.integer :occurences, default: 1
      t.integer :number_of_employees
      t.decimal :yearly_revenue, :precision => 20, :scale => 2
      t.text :trade_style_names
      t.text :industry_codes
    end
  end
end
