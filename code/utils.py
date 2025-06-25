
# Availability Service Status (Codes)
STATUS_CODES = [0, 1, 2]
# Availability Service Status (Description)
STATUSES = {
    0: "Served",
    1: "Underserved",
    2: "Unserved",
}

# Availability Data Types
AVAILABILITY_DTYPES = {
    "frn": str,
    "provider_id": int,
    "brand_name": str,
    "location_id": str,
    "technology": int,
    "max_advertised_download_speed": int,
    "max_advertised_upload_speed": int,
    "low_latency": int,
    "business_residential_code": str,
    "state_usps": str,
    "state_geoid": str,
    "county_geoid": str,
    "tract_geoid": str,
    "block_group_geoid": str,
    "block_geoid": str,
    "h3_res8_id": str,
    "status": int,
}

# Challenge Data Types
CHALLENGE_DTYPES = {
    "challenge_id": int,
    "location_id": str,
    "location_state": str,
    "data_vintage": str,
    "frn": str,
    "provider_id": int,
    "provider_brand_name": str,
    "holding_company_name": str,
    "technology": int,
    "category_code": int,
    "category_code_desc": str,
    "request_date": str,
    "request_method_code_desc": str,
    "date_received": str,
    "withdraw_date": str,
    "outcome": str,
    "outcome_code": int,
    "adjudication_date": str,
    "adjudication_code": str,
    "adjudication_code_desc": str,
    "state_geoid": str,
    "county_geoid": str,
    "tract_geoid": str,
    "block_group_geoid": str,
    "block_geoid": str,
}

# Access Technologies (Codes)
TECHNOLOGY_CODES = [10, 40, 50, 60, 61, 70, 71, 72, 0]
RELIABLE_TECHNOLOGY_CODES = [10, 40, 50, 71, 72]
# Access Technologies (Code: Full description)
TECHNOLOGIES = {
    10: "[10] Copper Wire",
    40: "[40] Coaxial Cable - HFC",
    50: "[50] Optical Carrier - Fiber to the Premises",
    60: "[60] Geostationary Satellite",
    61: "[61] Non-geostationary Satellite",
    70: "[70] Unlicensed Terrestrial Fixed Wireless",
    71: "[71] Licensed Terrestrial Fixed Wireless",
    72: "[72] Licensed-by-Rule Terrestrial Fixed Wireless",
    0:  "[00] Other",
}
# Access Technologies (Code: Short description)
TECHNOLOGIES_SHORT = {
    10: "[10] Copper",
    40: "[40] Cable",
    50: "[50] Fiber",
    60: "[60] GS Sat",
    61: "[61] NGS Sat",
    70: "[70] UTF Wireless",
    71: "[71] LTF Wireless",
    72: "[72] LbRTF Wireless",
    0:  "[00] Other",
}

# Challenge Outcomes (Codes)
OUTCOME_CODES = [0, 1, 2]
# Challenge Outcomes (Code: Description)
OUTCOMES = {
    0: "Upheld",
    1: "Overturned",
    2: "Withdrawn",
}
# Challenge Outcomes (Code: Short Description)
OUTCOMES_SHORT = {
    0: "U",
    1: "O",
    2: "W",
}

# Challenge Categories (Codes)
CATEGORY_CODES = [1, 2, 3, 4, 5, 6, 8, 9]
# Challenge Categories (Code: Full Description)
CATEGORIES = {
    1: "[1] Provider failed to schedule a service installation within 10"
       " business days of request",
    2: "[2] Provider did not install the service at the agreed-upon time",
    3: "[3] Provider requested more than the standard installation fee to"
       " connect service",
    4: "[4] Provider denied a request for service",
    5: "[5] Provider does not offer the technology reported to be available at"
       " this location",
    6: "[6] Provider does not offer the speed(s) reported to be available at"
       "this location",
    8: "[8] A wireless or satellite signal is not available at this location",
    9: "[9] Provider needed to construct new equipment at this location",
}
# Challenge Categories (Code: Short Description)
CATEGORIES_SHORT = {
    1: "[1] No Appt",
    2: "[2] No Show",
    3: "[3] Extra Fee",
    4: "[4] Service Denied",
    5: "[5] No Technology",
    6: "[6] No Speed",
    8: "[8] No Signal",
    9: "[9] New Equipment",
}
