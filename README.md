
**NASDAQ Trade Data Parser - Hourly VWAP Calculator**

**Description:**
This Python script to  parses NASDAQ ITCH 5.0 tick data files and calculates a running volume-weighted average price (VWAP) for each stock at every hour.

<b>Sample data link:</b> https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz

<b>Specification:</b> https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf

**Requirements:**
- Python 3.11

**Usage:**
To run the script, use the following command:
```python3.11 nasdaq_trade_data_parser.py file_path```


**Output:**
The script generates a JSON file (file_name.json) with the following format:
```json
{
    'symbol': {
        'hour1': price1,
        'hour2': price2,
        ...
    }
}