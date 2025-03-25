import pandas as pd

def clean_turbine_data(data):
    """Transform unprocessed data for processing, skipping if no cleaning is required."""
    if not data:
        return []
    
    cleaned_data = []
    for item in data:
        # Check if the item already has required fields (example condition)
        if "predicted" in item and item["predicted"] is True:
            cleaned_data.append(item)  # Skip cleaning, keep as is
        else:
            cleaned_data.append({**item, "predicted": True})  # Apply transformation

    return cleaned_data

def update_coordinates(data):
    """
    Update the Latitude and Longitude columns based on the MaintPlant column.
    If the country code is not recognized, determine the location using FunctionalLoc and Region.
    """

    df = pd.DataFrame(data)

    # Ensure required columns exist
    if 'MaintPlant' not in df.columns:
        print("Error: 'MaintPlant' column not found in data.")
        return data
    if 'Latitude' not in df.columns:
        df['Latitude'] = None
    if 'Longitude' not in df.columns:
        df['Longitude'] = None
    if 'FunctionalLoc' not in df.columns:
        df['FunctionalLoc'] = None
    if 'Region' not in df.columns:
        df['Region'] = None

    # Dictionary mapping country codes to coordinates
    country_info = {
    'AF': (33.9391, 67.7100),  # Afghanistan
    'AL': (41.1533, 20.1683),  # Albania
    'DZ': (28.0339, 1.6596),   # Algeria
    'AD': (42.5462, 1.6016),   # Andorra
    'AO': (-11.2027, 17.8739), # Angola
    'AG': (17.0608, -61.7964), # Antigua and Barbuda
    'AR': (-38.4161, -63.6167),# Argentina
    'AM': (40.0691, 45.0382),  # Armenia
    'AU': (-25.2744, 133.7751),# Australia
    'AT': (47.5162, 14.5501),  # Austria
    'AZ': (40.1431, 47.5769),  # Azerbaijan
    'BS': (25.0343, -77.3963), # Bahamas
    'BH': (26.0667, 50.5577),  # Bahrain
    'BD': (23.6850, 90.3563),  # Bangladesh
    'BB': (13.1939, -59.5432), # Barbados
    'BY': (53.9006, 27.5590),  # Belarus
    'BE': (50.8503, 4.3517),   # Belgium
    'BZ': (17.1899, -88.4976), # Belize
    'BJ': (9.3077, 2.3158),    # Benin
    'BT': (27.5142, 90.4336),  # Bhutan
    'BO': (-16.2902, -63.5887),# Bolivia
    'BA': (43.9159, 17.6791),  # Bosnia and Herzegovina
    'BW': (-22.3285, 24.6849), # Botswana
    'BR': (-14.2350, -51.9253),# Brazil
    'BN': (4.5353, 114.7277),  # Brunei
    'BG': (42.7339, 25.4858),  # Bulgaria
    'BF': (12.2383, -1.5616),  # Burkina Faso
    'BI': (-3.3731, 29.9189),  # Burundi
    'KH': (12.5657, 104.9910), # Cambodia
    'CM': (7.3697, 12.3547),   # Cameroon
    'CA': (56.1304, -106.3468),# Canada
    'CV': (16.5388, -23.0418), # Cape Verde
    'CF': (6.6111, 20.9394),   # Central African Republic
    'TD': (15.4542, 18.7322),  # Chad
    'CL': (-35.6751, -71.5430),# Chile
    'CN': (35.8617, 104.1954), # China
    'CO': (4.5709, -74.2973),  # Colombia
    'KM': (-11.6455, 43.3333), # Comoros
    'CG': (-0.2280, 15.8277),  # Congo (Brazzaville)
    'CD': (-4.0383, 21.7587),  # Congo (Kinshasa)
    'CR': (9.7489, -83.7534),  # Costa Rica
    'CI': (7.5399, -5.5471),   # Côte d'Ivoire
    'HR': (45.1000, 15.2000),  # Croatia
    'CU': (21.5218, -77.7812), # Cuba
    'CY': (35.1264, 33.4299),  # Cyprus
    'CZ': (49.8175, 15.4729),  # Czech Republic
    'DK': (56.2639, 9.5018),   # Denmark
    'DJ': (11.8251, 42.5903),  # Djibouti
    'DO': (18.7357, -70.1627), # Dominican Republic
    'EC': (-1.8312, -78.1834),# Ecuador
    'EG': (26.8206, 30.8025),  # Egypt
    'SV': (13.7942, -88.8965), # El Salvador
    'GQ': (1.6508, 10.2679),   # Equatorial Guinea
    'ER': (15.1794, 39.7823),  # Eritrea
    'EE': (58.5953, 25.0136),  # Estonia
    'ET': (9.1450, 40.4897),   # Ethiopia
    'FI': (61.9241, 25.7482),  # Finland
    'FR': (46.6034, 1.8883),   # France
    'DE': (51.1657, 10.4515),  # Germany
    'GH': (7.9465, -1.0232),   # Ghana
    'GR': (39.0742, 21.8243),  # Greece
    'GT': (15.7835, -90.2308), # Guatemala
    'HT': (18.9712, -72.2852), # Haiti
    'HU': (47.1625, 19.5033),  # Hungary
    'IN': (20.5937, 78.9629),  # India
    'ID': (-0.7893, 113.9213), # Indonesia
    'IR': (32.4279, 53.6880),  # Iran
    'IQ': (33.2232, 43.6793),  # Iraq
    'IE': (53.4129, -8.2439),  # Ireland
    'IL': (31.0461, 34.8516),  # Israel
    'IT': (41.8719, 12.5674),  # Italy
    'JP': (36.2048, 138.2529), # Japan
    'KE': (-1.286389, 36.817223), # Kenya
    'KR': (35.9078, 127.7669), # South Korea
    'MY': (4.2105, 101.9758),  # Malaysia
    'MX': (23.6345, -102.5528),# Mexico
    'NL': (52.1326, 5.2913),   # Netherlands
    'NZ': (-40.9006, 174.8860),# New Zealand
    'NG': (9.0820, 8.6753),    # Nigeria
    'NO': (60.4720, 8.4689),   # Norway
    'PK': (30.3753, 69.3451),  # Pakistan
    'PH': (12.8797, 121.7740), # Philippines
    'PL': (51.9194, 19.1451),  # Poland
    'PT': (39.3999, -8.2245),  # Portugal
    'RU': (61.5240, 105.3188), # Russia
    'ZA': (-30.5595, 22.9375), # South Africa
    'ES': (40.4637, -3.7492),  # Spain
    'SE': (60.1282, 18.6435),  # Sweden
    'CH': (46.8182, 8.2275),   # Switzerland
    'TH': (15.8700, 100.9925), # Thailand
    'TR': (38.9637, 35.2433),  # Turkey
    'UA': (48.3794, 31.1656),  # Ukraine
    'GB': (55.3781, -3.4360),  # United Kingdom
    'US': (37.0902, -95.7129), # United States
    'VN': (14.0583, 108.2772)  # Vietnam
}

    # Dictionary mapping FunctionalLoc prefix to country
    functional_loc_country = {
    'AF': 'Afghanistan', 'AL': 'Albania', 'DZ': 'Algeria', 'AD': 'Andorra', 'AO': 'Angola', 
    'AG': 'Antigua and Barbuda', 'AR': 'Argentina', 'AM': 'Armenia', 'AU': 'Australia', 'AT': 'Austria', 
    'AZ': 'Azerbaijan', 'BS': 'Bahamas', 'BH': 'Bahrain', 'BD': 'Bangladesh', 'BB': 'Barbados', 
    'BY': 'Belarus', 'BE': 'Belgium', 'BZ': 'Belize', 'BJ': 'Benin', 'BT': 'Bhutan', 
    'BO': 'Bolivia', 'BA': 'Bosnia and Herzegovina', 'BW': 'Botswana', 'BR': 'Brazil', 'BN': 'Brunei', 
    'BG': 'Bulgaria', 'BF': 'Burkina Faso', 'BI': 'Burundi', 'KH': 'Cambodia', 'CM': 'Cameroon', 
    'CA': 'Canada', 'CV': 'Cape Verde', 'CF': 'Central African Republic', 'TD': 'Chad', 'CL': 'Chile', 
    'CN': 'China', 'CO': 'Colombia', 'KM': 'Comoros', 'CD': 'Congo (Democratic Republic)', 'CG': 'Congo (Republic)', 
    'CR': 'Costa Rica', 'HR': 'Croatia', 'CU': 'Cuba', 'CY': 'Cyprus', 'CZ': 'Czechia', 
    'DK': 'Denmark', 'DJ': 'Djibouti', 'DM': 'Dominica', 'DO': 'Dominican Republic', 'EC': 'Ecuador', 
    'EG': 'Egypt', 'SV': 'El Salvador', 'GQ': 'Equatorial Guinea', 'ER': 'Eritrea', 'EE': 'Estonia', 
    'ET': 'Ethiopia', 'FJ': 'Fiji', 'FI': 'Finland', 'FR': 'France', 'GA': 'Gabon', 
    'GM': 'Gambia', 'GE': 'Georgia', 'DE': 'Germany', 'GH': 'Ghana', 'GR': 'Greece', 
    'GD': 'Grenada', 'GT': 'Guatemala', 'GN': 'Guinea', 'GW': 'Guinea-Bissau', 'GY': 'Guyana', 
    'HT': 'Haiti', 'HN': 'Honduras', 'HU': 'Hungary', 'IS': 'Iceland', 'IN': 'India', 
    'ID': 'Indonesia', 'IR': 'Iran', 'IQ': 'Iraq', 'IE': 'Ireland', 'IL': 'Israel', 
    'IT': 'Italy', 'JM': 'Jamaica', 'JP': 'Japan', 'JO': 'Jordan', 'KZ': 'Kazakhstan', 
    'KE': 'Kenya', 'KI': 'Kiribati', 'KW': 'Kuwait', 'KG': 'Kyrgyzstan', 'LA': 'Laos', 
    'LV': 'Latvia', 'LB': 'Lebanon', 'LS': 'Lesotho', 'LR': 'Liberia', 'LY': 'Libya', 
    'LI': 'Liechtenstein', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'MG': 'Madagascar', 'MW': 'Malawi', 
    'MY': 'Malaysia', 'MV': 'Maldives', 'ML': 'Mali', 'MT': 'Malta', 'MH': 'Marshall Islands', 
    'MR': 'Mauritania', 'MU': 'Mauritius', 'MX': 'Mexico', 'FM': 'Micronesia', 'MD': 'Moldova', 
    'MC': 'Monaco', 'MN': 'Mongolia', 'ME': 'Montenegro', 'MA': 'Morocco', 'MZ': 'Mozambique', 
    'MM': 'Myanmar', 'NA': 'Namibia', 'NR': 'Nauru', 'NP': 'Nepal', 'NL': 'Netherlands', 
    'NZ': 'New Zealand', 'NI': 'Nicaragua', 'NE': 'Niger', 'NG': 'Nigeria', 'KP': 'North Korea', 
    'MK': 'North Macedonia', 'NO': 'Norway', 'OM': 'Oman', 'PK': 'Pakistan', 'PW': 'Palau', 
    'PA': 'Panama', 'PG': 'Papua New Guinea', 'PY': 'Paraguay', 'PE': 'Peru', 'PH': 'Philippines', 
    'PL': 'Poland', 'PT': 'Portugal', 'QA': 'Qatar', 'RO': 'Romania', 'RU': 'Russia', 
    'RW': 'Rwanda', 'WS': 'Samoa', 'SM': 'San Marino', 'ST': 'Sao Tome and Principe', 'SA': 'Saudi Arabia', 
    'SN': 'Senegal', 'RS': 'Serbia', 'SC': 'Seychelles', 'SL': 'Sierra Leone', 'SG': 'Singapore', 
    'SK': 'Slovakia', 'SI': 'Slovenia', 'SB': 'Solomon Islands', 'SO': 'Somalia', 'ZA': 'South Africa', 
    'KR': 'South Korea', 'SS': 'South Sudan', 'ES': 'Spain', 'LK': 'Sri Lanka', 'SD': 'Sudan', 
    'SR': 'Suriname', 'SE': 'Sweden', 'CH': 'Switzerland', 'SY': 'Syria', 'TW': 'Taiwan', 
    'TJ': 'Tajikistan', 'TZ': 'Tanzania', 'TH': 'Thailand', 'TG': 'Togo', 'TO': 'Tonga', 
    'TT': 'Trinidad and Tobago', 'TN': 'Tunisia', 'TR': 'Turkey', 'TM': 'Turkmenistan', 'TV': 'Tuvalu', 
    'UG': 'Uganda', 'UA': 'Ukraine', 'AE': 'United Arab Emirates', 'GB': 'United Kingdom', 'US': 'United States', 
    'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VU': 'Vanuatu', 'VA': 'Vatican City', 'VE': 'Venezuela', 
    'VN': 'Vietnam', 'YE': 'Yemen', 'ZM': 'Zambia', 'ZW': 'Zimbabwe'
}


    # Dictionary mapping Region to general coordinates (fallback)
    region_coordinates = {
    'North America': (45.0000, -100.0000),      # USA, Canada, Mexico
    'Central America': (10.0000, -85.0000),     # Guatemala, Costa Rica, Panama, etc.
    'Caribbean': (20.0000, -75.0000),           # Cuba, Jamaica, Haiti, etc.
    'South America': (-15.0000, -60.0000),      # Brazil, Argentina, Chile, etc.
    'Latin America': (-10.0000, -55.0000),      # Combines Central & South America (broader region)
    'Western Europe': (50.0000, 5.0000),        # France, Germany, UK, etc.
    'Eastern Europe': (55.0000, 25.0000),       # Poland, Ukraine, Russia (West), etc.
    'Northern Europe': (60.0000, 15.0000),      # Scandinavia, Baltic States
    'Southern Europe': (40.0000, 15.0000),      # Spain, Italy, Greece, etc.
    'Europe': (50.0000, 10.0000),               # Generalized Europe region
    'North Africa': (25.0000, 10.0000),         # Egypt, Algeria, Libya, etc.
    'Sub-Saharan Africa': (-5.0000, 20.0000),   # Nigeria, Kenya, South Africa, etc.
    'West Africa': (10.0000, -5.0000),          # Ghana, Senegal, Ivory Coast, etc.
    'East Africa': (-2.0000, 35.0000),          # Kenya, Ethiopia, Tanzania, etc.
    'Central Africa': (0.0000, 20.0000),        # Congo, Gabon, Chad, etc.
    'Southern Africa': (-20.0000, 25.0000),     # South Africa, Namibia, Botswana, etc.
    'Middle East': (25.0000, 50.0000),          # Saudi Arabia, UAE, Iran, etc.
    'West Asia': (30.0000, 45.0000),            # Turkey, Iran, Iraq, etc.
    'Central Asia': (40.0000, 70.0000),         # Kazakhstan, Uzbekistan, Turkmenistan, etc.
    'South Asia': (20.0000, 80.0000),           # India, Pakistan, Bangladesh, Sri Lanka
    'Southeast Asia': (10.0000, 110.0000),      # Indonesia, Philippines, Vietnam, etc.
    'East Asia': (35.0000, 120.0000),           # China, Japan, Korea, Taiwan
    'Asia': (30.0000, 100.0000),                # Generalized Asia region
    'Oceania': (-10.0000, 160.0000),            # Australia, New Zealand, Pacific Islands
    'Australia': (-25.0000, 135.0000),          # Australia (specific)
    'Pacific Islands': (-5.0000, 170.0000),     # Fiji, Papua New Guinea, Samoa, etc.
    'Arctic': (75.0000, 0.0000),                # Arctic region (Polar)
    'Antarctica': (-75.0000, 0.0000)            # Antarctic region (Polar)
}


    # Extract the first two characters of MaintPlant to get the country code
    df['CountryCode'] = df['MaintPlant'].str[:2]

    # First attempt to get coordinates from country code
    df[['Latitude', 'Longitude']] = df['CountryCode'].apply(lambda x: country_info.get(x, (None, None))).apply(pd.Series)

    # If Latitude is missing, try to use FunctionalLoc
    missing_lat_long = df['Latitude'].isna()

    if missing_lat_long.any():
        # Extract FunctionalLoc prefix
        df.loc[missing_lat_long, 'FunctionalLocPrefix'] = df.loc[missing_lat_long, 'FunctionalLoc'].str[:2]

        # Map FunctionalLocPrefix to Country
        df.loc[missing_lat_long, 'CountryFromFunctionalLoc'] = df.loc[missing_lat_long, 'FunctionalLocPrefix'].map(functional_loc_country)

        # Get coordinates for that country
        df.loc[missing_lat_long, ['Latitude', 'Longitude']] = df.loc[missing_lat_long, 'CountryFromFunctionalLoc'].map(lambda x: country_info.get(x, (None, None))).apply(pd.Series)

    # If Latitude is still missing, try using the Region
    missing_lat_long = df['Latitude'].isna()

    if missing_lat_long.any():
        df.loc[missing_lat_long, ['Latitude', 'Longitude']] = df.loc[missing_lat_long, 'Region'].map(lambda x: region_coordinates.get(x, (None, None))).apply(pd.Series)

    # Drop helper columns
    df.drop(columns=['CountryCode', 'FunctionalLocPrefix', 'CountryFromFunctionalLoc'], errors='ignore', inplace=True)

    return df.to_dict(orient='records')
