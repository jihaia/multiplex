require 'csv'
DnB::Direct::Plus.use_credentials 'AUvFfV4HmnM1kaGyUoRCMgA99DmtGE6n', 'GYPkpLB5SwsdNEVg'

def get_sic8(industry_codes=[])
  code = nil
  industry_codes.each do |entry|
    if entry["typeDnBCode"] == 3599 && entry["priority"] == 1
      code = entry["code"]
      break
    end
  end
  code
end

def get_entity_type(et={})
  et.nil? ? nil : et["dnbCode"]
end

def get_location_type(linkage)
  nil
end

def get_parent_duns(cl={})
  parent = cl["parent"] || {}
  parent["duns"]
end

def get_parent_name(cl={})
  parent = cl["parent"] || {}
  parent["primaryName"]
end

def get_gu_duns(cl={})
  gu = cl["globalUltimate"] || {}
  gu["duns"]
end

def get_gu_name(cl={})
  gu = cl["globalUltimate"] || {}
  gu["primaryName"]
end

def get_du_duns(cl={})
  du = cl["domesticUltimate"] || {}
  du["duns"]
end

def get_du_name(cl={})
  du = cl["domesticUltimate"] || {}
  du["primaryName"]
end

def get_fips(address)
  address["addressCountry"]["fipsCode"] if (address.is_a?(Hash) && address["addressCountry"].is_a?(Hash))
end

def get_is_delisted(dcs={})
  dcs["isDelisted"]
end

def get_sales_rev_year(financials=[])
    first = financials[0] || {}
    return first["financialStatementToDate"]
end

def get_sales_rev_usd(financials=[])
    usd = nil
    first = financials[0] || {}

    yearly = first["yearlyRevenue"] || []

    yearly.each do |yr|
      usd = yr["value"] if yr["currency"] == "USD"
    end
    return usd
end

def get_operating_status(op_status={})
  op_status["dnbCode"]
end


def get_po_box(org)
  pb = nil
  # fetch from primary address
  pa = org["primaryAddress"] || {}
  pa_pb = pa["postOfficeBox"] || {}
  pb = pa_pb["postOfficeBoxNumber"]

  # check on the mailing address if missing
  if pb.nil?
    ma = org["mailingAddress"] || {}
    ma_pb = ma["postOfficeBox"] || {}
    pb = ma_pb["postOfficeBoxNumber"]
  end

  return pb
end

def get_num_employees_here(number_of_employees=[])
  first = number_of_employees[0] || {}
  first["value"]
end

def get_isd_code(tele=[])
  first = tele[0] || {}
  first["isdCode"]
end

def get_telephone(tele=[])
  first = tele[0] || {}
  first["telephoneNumber"]
end

def get_confidence_code(mqi={})
  mqi["confidenceCode"]
end

def get_match_grade(mqi)
  mqi["matchGrade"]
end

def get_match_data_profile(mqi)
  mqi["matchDataProfile"]
end

namespace :msft do

    desc 'Reads source and seeds cds import'
    task seed: :environment do

      row_ctr = 0
      header = []
      file_in = File.join("/Users/jihaia", "Dropbox/DnB/customers/apple", "matched-std.json")
      file_out = File.join("/Users/jihaia", "Dropbox/DnB/customers/apple", "seeded.csv")

      CSV.open(file_out, "wb") do |csv|
        csv << [
          "duns",
          "primaryName",
          "tradeStyleName",
          "streetAddress1",
          "streetAddress2",
          "city",
          "stateProvince",
          "postalCode",
          "countryCode",
          "fipsCode",
          "poBox",
          "primarySic",
          "businessEntityType",
          "locationType",
          "parentDuns",
          "parentName",
          "domesticUltimateDuns",
          "domesticUltimateName",
          "globalUltimateDuns",
          "globalUltimateName",
          "isDelisted",
          "salesRevenueYear",
          "salesRevenueAmountUSD",
          "operatingStatus",
          "numberOfEmployeesHere",
          "numberOfEmployeesTotal",
          "isdCode",
          "telephoneNumber",
          "confidenceCode",
          "matchGrade",
          "matchDataProfile"
        ]

        File.foreach(file_in) do |line|

          row_ctr += 1

          resp = JSON.parse(line)
          begin

            next if (resp["embeddedProduct"].nil? || resp["embeddedProduct"]["organization"].nil?)
          org = resp["embeddedProduct"]["organization"]

          # Trade Style
          trade_style = ""
          if org["tradeStyleNames"].is_a? Array
            if org["tradeStyleNames"][0]
              trade_style = org["tradeStyleNames"][0]["name"]
            end
          end

          # Address line 1
          address_line_1 = ""
          if org["primaryAddress"] && org["primaryAddress"]["streetAddress"]
            address_line_1 = org["primaryAddress"]["streetAddress"]["line1"]
          end

          # Address line 2
          address_line_2 = ""
          if org["primaryAddress"] && org["primaryAddress"]["streetAddress"]
            address_line_2 = org["primaryAddress"]["streetAddress"]["line2"]
          end

          # City
          city = ""
          if org["primaryAddress"] && org["primaryAddress"]["addressLocality"]
            city = org["primaryAddress"]["addressLocality"]["name"]
          end


          # State Province
          state_province = ""
          if org["primaryAddress"] && org["primaryAddress"]["addressRegion"]
            state_province = org["primaryAddress"]["addressRegion"]["name"]
          end

          # Postal Code
          postal_code = ""
          if org["primaryAddress"]
            postal_code = org["primaryAddress"]["postalCode"]
          end

          # Country Code
          country_code = ""
          if org["primaryAddress"] && org["primaryAddress"]["addressCountry"]
            country_code = org["primaryAddress"]["addressCountry"]["isoAlpha2Code"]
          end

          # fipsCode
          fips_code = get_fips(org["primaryAddress"])

          # poBox
          po_box = get_po_box(org)

          # Primary SIC-8
          primary_sic = get_sic8(org["industryCodes"] || [])

          # businessEntityType
          entity_type = get_entity_type(org["businessEntityType"])

          # locationType
          location_type = get_location_type(org["corporateLinkage"])

          # Parent DUNS
          parent_duns = get_parent_duns(org["corporateLinkage"])

          # Parent Name
          parent_name = get_parent_name(org["corporateLinkage"])

          # DU DUNS
          du_duns = get_du_duns(org["corporateLinkage"])

          # DU Name
          du_name = get_du_name(org["corporateLinkage"])

          # GU DUNS
          gu_duns = get_gu_duns(org["corporateLinkage"])

          # GU Name
          gu_name = get_gu_name(org["corporateLinkage"])

          # isDelisted
          is_delisted = get_is_delisted(org["dunsControlStatus"])

          # salesRevenueYear
          sales_rev_year = get_sales_rev_year(org["financials"])

          # salesRevenueAmountUSD
          sales_rev_amt_usd = get_sales_rev_usd(org["financials"])

          # operatingStatus
          operating_status = get_operating_status(org["dunsControlStatus"]["operatingStatus"])

          # numberOfEmployeesHere
          num_emp_here = get_num_employees_here(org["numberOfEmployees"])

          # numberOfEmployeesTotal
          num_emp_total = nil

          # isdCode
          isd_code = get_isd_code(org["telephone"])

          # telephoneNumber
          telephone_number = get_telephone(org["telephone"])

          mqi = resp["matchCandidates"][0]["matchQualityInformation"]

          # confidenceCode
          confidence_code = get_confidence_code(mqi)

          # matchGrade
          match_grade = get_match_grade(mqi)

          # matchDataProfile
          mdp = get_match_data_profile(mqi)


          csv << [
            org["duns"],
            org["primaryName"],
            trade_style,
            address_line_1,
            address_line_2,
            city,
            state_province,
            postal_code,
            country_code,
            fips_code,
            po_box,
            primary_sic,
            entity_type,
            location_type,
            parent_duns,
            parent_name,
            du_duns,
            du_name,
            gu_duns,
            gu_name,
            is_delisted,
            sales_rev_year,
            sales_rev_amt_usd,
            operating_status,
            num_emp_here,
            num_emp_total,
            isd_code,
            telephone_number,
            confidence_code,
            match_grade,
            mdp.to_s
          ]
        rescue => ex
          p ex.message, ex.backtrace
          
          end



          if row_ctr % 100 == 0
            p "[Processed] #{row_ctr} rows so far"
            csv.flush
          end


          break if row_ctr == 20000
        end
      end
      p "[Finished] #{row_ctr} total rows"
    end

end # msft
