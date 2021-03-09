# easy-census-geocode
Interactive geocoder interface with US Census Bureau API

Simple python program to interface with the [US Census Bureau API](https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/census-geocoder.html).

It can no doubt be extended to bring in other parts of the API functionality. At the moment, it's only written to get basic address-level geographies (county, block, track, etc.). I find it useful as it automates the process, which for large files is very helpful -- especially so in fact as the batch encode upload on the website has a limit of 10,000 addresses per file.
