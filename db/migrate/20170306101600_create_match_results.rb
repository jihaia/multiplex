class CreateMatchResults < ActiveRecord::Migration
  def change
    create_table :match_results do |t|
      t.string :source_id
      t.string :status
      t.string :duns
      t.string :confidence_code
      t.string :match_grade
    end
  end
end
