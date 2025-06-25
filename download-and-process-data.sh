
###############################################################################
# 1. DATA DOWNLOAD ############################################################
###############################################################################
# 1.1 Download BDC availability data
time python3 code/download-bdc-availability.py

# 1.2 Download BDC challenge data
time python3 code/download-bdc-challenge.py

###############################################################################
# 2. DATA PROCESSING ##########################################################
###############################################################################
# 2.1 Process BDC availability data. Depends on:
# - code/download-bdc-availability.py
time python3 code/process-bdc-availability.py

# 2.2 Determine BSL geolocations from availability. Depends on:
# - code/process-bdc-availability.py
time python3 code/determine-bsl-geolocation.py

# 2.3 Process BDC challenge data. Depends on:
# - code/download-bdc-challenge.py
# - code/determine-bsls-from-availability.py
time python3 code/process-bdc-challenge.py

# 2.4 Extract availability data for challenging BSLs. Depends on:
# - code/process-bdc-challenge.py
# - code/process-bdc-availability.py
time python3 code/extract-cbsl-availability.py

###############################################################################
# 3. DATA SUMMARIZATION #######################################################
###############################################################################
# 3.1 Summarize BDC challenge data per geographic unit. Depends on:
# - code/process-bdc-challenge.py
time python3 code/summarize-challenges-per-geo.py

# 3.2 Summarize BDC challenge data per BSL. Depends on:
# - code/process-bdc-challenge.py
time python3 code/summarize-challenges-per-bsl.py

# 3.3 Summarize BDC availability data per geographic unit. Depends on:
# - code/process-bdc-availability.py
time python3 code/summarize-availability-per-geo.py

# 3.4 Summarize BDC availability data per challenging BSL. Depends on:
# - code/extract-cbsl-availability.py
time python3 code/summarize-availability-per-cbsl.py

# 3.5 Merge challenge and availability summary data. Depends on:
# - code/summarize-challenges-per-geo.py
# - code/summarize-challenges-per-bsl.py
# - code/summarize-availability-per-geo.py
# - code/summarize-availability-per-cbsl.py
time python3 code/merge-challenge-availability-summaries.py