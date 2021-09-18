# easy-census-geocode
Extension of the US Census Bureau API geocoder.

Python program to make working with the [US Census Bureau API](https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/census-geocoder.html) easier. The main advangates of this script are:

- Using try/catch and avoiding stopping the entire process when one request fails. It moves failed requests to a queue to be processed at the end. This is helpful if some of the raw addresses in your dataset are incorrectly formatted
- At the end, process failed requests, splitting the data recursively to find the problematic addresses
- Paralelize requests to improve performance (the improvement will depend on how the geocode server process the requests, if they process all in parallel this can greatly improve performance)
- Cache and save results so you don't need to reprocess already processed addresses if you run the script again

I've found this script to be especially useful when you have large data files which exceed the 10,000 address batch limit of the API. It will automatically split your large file down to smaller chunks and append the results once finished processing.
