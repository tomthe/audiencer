#create input
#%%

mapping = {
        #"AD": "Andorra",
        "AE": "United Arab Emirates",
        "AF": "Afghanistan",
        #"AG": "Antigua and Barbuda",
        "AL": "Albania",
        "AM": "Armenia",
        "AO": "Angola",
        "AR": "Argentina",
        #"AS": "American Samoa",
        "AT": "Austria",
        "AU": "Australia",
        "AW": "Aruba",
        "AZ": "Azerbaijan",
        "BA": "Bosnia and Herzegovina",
        "BB": "Barbados",
        "BD": "Bangladesh",
        "BE": "Belgium",
        "BF": "Burkina Faso",
        "BG": "Bulgaria",
        "BH": "Bahrain",
        "BI": "Burundi",
        "BJ": "Benin",
        "BM": "Bermuda",
        "BN": "Brunei",
        "BO": "Bolivia",
        "BR": "Brazil",
        "BS": "Bahamas",
        "BT": "Bhutan",
        "BW": "Botswana",
        "BY": "Belarus",
        "BZ": "Belize",
        "CA": "Canada",
        "CD": "Congo Dem. Rep.",
        "CF": "Central African Republic",
        "CG": "Congo Rep.",
        "CH": "Switzerland",
        "CI": "Cote d'Ivoire",
        "CK": "Cook Islands",
        "CL": "Chile",
        "CM": "Cameroon",
        "CN": "China",
        "CO": "Colombia",
        "CR": "Costa Rica",
        "CV": "Cape Verde",
        "CW": "Curacao",
        "CY": "Cyprus",
        "CZ": "Czech Republic",
        "DE": "Germany",
        "DJ": "Djibouti",
        "DK": "Denmark",
        "DM": "Dominica",
        "DO": "Dominican Republic",
        "DZ": "Algeria",
        "EC": "Ecuador",
        "EE": "Estonia",
        "EG": "Egypt",
        "EH": "Western Sahara",
        "ER": "Eritrea",
        "ES": "Spain",
        "ET": "Ethiopia",
        "FI": "Finland",
        "FJ": "Fiji",
        "FK": "Falkland Islands",
        "FM": "Micronesia",
        #"FO": "Faroe Islands",
        "FR": "France",
        "GA": "Gabon",
        "GB": "United Kingdom",
        "GD": "Grenada",
        "GE": "Georgia",
        #"GF": "French Guiana",
        #"GG": "Guernsey",
        "GH": "Ghana",
        "GI": "Gibraltar",
        "GL": "Greenland",
        "GM": "Gambia",
        "GN": "Guinea-Bissau",
        #"GP": "Guadeloupe",
        "GQ": "Equatorial Guinea",
        "GR": "Greece",
        "GT": "Guatemala",
        "GU": "Guam",
        "GW": "Guinea",
        "GY": "Guyana",
        "HK": "Hong Kong",
        "HN": "Honduras",
        "HR": "Croatia",
        "HT": "Haiti",
        "HU": "Hungary",
        "ID": "Indonesia",
        "IE": "Ireland",
        "IL": "Israel",
        #"IM": "Isle of Man",
        "IN": "India",
        "IQ": "Iraq",
        "IR": "Iran",
        "IS": "Iceland",
        "IT": "Italy",
        #"JE": "Jersey",
        "JM": "Jamaica",
        "JO": "Jordan",
        "JP": "Japan",
        "KE": "Kenya",
        "KG": "Kyrgyzstan",
        "KH": "Cambodia",
        #"KI": "Kiribati",
        "KM": "Comoros",
        #"KN": "Saint Kitts and Nevis",
        "KR": "South Korea",
        "KW": "Kuwait",
        "KY": "Cayman Islands",
        "KZ": "Kazakhstan",
        "LA": "Laos",
        "LB": "Lebanon",
        "LC": "Saint Lucia",
        #"LI": "Liechtenstein",
        "LK": "Sri Lanka",
        "LR": "Liberia",
        "LS": "Lesotho",
        "LT": "Lithuania",
        "LU": "Luxembourg",
        "LV": "Latvia",
        "LY": "Libya",
        "MA": "Morocco",
        "MC": "Monaco",
        "MD": "Moldova",
        "ME": "Montenegro",
        #"MF": "Saint Martin",
        "MG": "Madagascar",
        "MH": "Marshall Islands",
        "MK": "Macedonia",
        "ML": "Mali",
        "MM": "Myanmar",
        "MN": "Mongolia",
        "MO": "Macau",
        "MP": "Northern Mariana Islands",
        "MQ": "Martinique",
        "MR": "Mauritania",
        "MS": "Montserrat",
        "MT": "Malta",
        "MU": "Mauritius",
        "MV": "Maldives",
        "MW": "Malawi",
        "MX": "Mexico",
        "MY": "Malaysia",
        "MZ": "Mozambique",
        "NA": "Namibia",
        "NC": "New Caledonia",
        "NE": "Niger",
        #"NF": "Norfolk Island",
        "NG": "Nigeria",
        "NI": "Nicaragua",
        "NL": "Netherlands",
        "NO": "Norway",
        "NP": "Nepal",
        "NR": "Nauru",
        #"NU": "Niue",
        "NZ": "New Zealand",
        "OM": "Oman",
        "PA": "Panama",
        "PE": "Peru",
        "PF": "French Polynesia",
        "PG": "Papua New Guinea",
        "PH": "Philippines",
        "PK": "Pakistan",
        "PL": "Poland",
        #"PM": "Saint Pierre and Miquelon",
        "PN": "Pitcairn",
        "PR": "Puerto Rico",
        "PS": "Palestine",
        "PT": "Portugal",
        "PW": "Palau",
        "PY": "Paraguay",
        "QA": "Qatar",
        "RE": "Reunion",
        "RO": "Romania",
        "RS": "Serbia",
        #"RU": "Russia",
        "RW": "Rwanda",
        "SA": "Saudi Arabia",
        #"SB": "Solomon Islands",
        "SC": "Seychelles",
        "SE": "Sweden",
        "SG": "Singapore",
        #"SH": "Saint Helena",
        "SI": "Slovenia",
        #"SJ": "Svalbard and Jan Mayen",
        "SK": "Slovakia",
        "SL": "Sierra Leone",
        "SM": "San Marino",
        "SN": "Senegal",
        "SO": "Somalia",
        "SR": "Suriname",
        "SS": "South Sudan",
        #"ST": "Sao Tome and Principe",
        "SV": "El Salvador",
        #"SY": "Syria",
        #"SX": "Sint Maarten",
        "SZ": "Swaziland",
        #"TC": "Turks and Caicos Islands",
        "TD": "Chad",
        "TG": "Togo",
        "TH": "Thailand",
        "TJ": "Tajikistan",
        "TK": "Tokelau",
        "TL": "Timor-Leste",
        "TM": "Turkmenistan",
        "TN": "Tunisia",
        "TO": "Tonga",
        "TR": "Turkey",
        "TT": "Trinidad and Tobago",
        "TV": "Tuvalu",
        "TW": "Taiwan",
        "TZ": "Tanzania",
        "UA": "Ukraine",
        "UG": "Uganda",
        "US": "United States",
        "UY": "Uruguay",
        "UZ": "Uzbekistan",
        #"VC": "Saint Vincent and the Grenadines",
        "VE": "Venezuela",
        #"VG": "British Virgin Islands",
        #"VI": "US Virgin Islands",
        "VN": "Vietnam",
        #"VU": "Vanuatu",
        #"WF": "Wallis and Futuna",
        "WS": "Samoa",
        "XK": "Kosovo",
        "YE": "Yemen",
        #"YT": "Mayotte",
        "ZA": "South Africa",
        "ZM": "Zambia",
        "ZW": "Zimbabwe",
    }

print(len(mapping))
countrycodes = list(mapping.keys())
#%%


input_data_json = {
    "name": "worldwide expat collection",
    "geo_locations": [],
    "genders": [1,2],
    "ages_ranges": [
        {"min":18, "max":39},
        {"min":40}
    ],
    "behavior": [
    ],
	"scholarities": [{
			"name": "Graduated",
			"or": [3, 7, 8, 9, 11]
		},
		# {
		# 	"name": "No Degree",
		# 	"or": [1, 13]
		# }, {
		# 	"name": "High School",
		# 	"or": [2, 4, 5, 6, 10]
		# }
	],

}

for countrycode in countrycodes:
    input_data_json["geo_locations"].append(
        { "name": "countries", "values": [countrycode],  "location_types": ["home"] }
    )


import pandas as pd
expats = pd.read_csv("./facebook_behavior_expat_origin.csv", header=0)
print(expats.head(3))

for behavior in range(0, len(expats["key"])):
    if expats["origin"][behavior] in ["Expats (Luxembourg)","Expats (Greece)","Expats (Latvia)","Expats (Slovenia)","Expats (Malta)","Expats (Monaco)"]:
        continue
    input_data_json["behavior"].append(
        {"name": expats["origin"][behavior], "or": [int(expats["key"][behavior])]}
    )

#print(input_data_json)
print(len(input_data_json["behavior"]),\
    len(input_data_json["geo_locations"]),len(input_data_json["genders"]),\
    len(input_data_json["scholarities"]),len(input_data_json["ages_ranges"]))
#%%
print(len(input_data_json["behavior"])*\
        len(input_data_json["geo_locations"])* (len(input_data_json["genders"])+1)*\
        (len(input_data_json["scholarities"])+1)*(len(input_data_json["ages_ranges"])+1))
#%%
print(len(input_data_json["behavior"])*\
        len(input_data_json["geo_locations"])* ((len(input_data_json["genders"])+1)+\
        (len(input_data_json["scholarities"])+1)+(len(input_data_json["ages_ranges"])+1)))
#%%
# %%
import json
with open("input_data_whole_world.json", "w") as outfile:
    json.dump(input_data_json, outfile)
# %%
