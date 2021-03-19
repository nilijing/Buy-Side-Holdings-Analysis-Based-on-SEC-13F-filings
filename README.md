# Buy-Side-Holdings-Analysis-Based-on-SEC-13F-filings
Scrape and parse funds’ SEC 13F quarterly reports from the SEC's EDGAR database. Take 2 funds Blackstone and Two Sigma as examples.

The database named 'sec_reports' has 3 tables: 'holdings', 'date_records', and 'ciks' are separately for downloading reports, recording reports' dates, and mapping cik to ticker.
 
First, 'reports.py' created the original 'holdings' table which included 2 funds (Blackstone and Two Sigma) reports.
In this step, I also used 'dates.py' to record the reports' ciks and submitted dates. 
The 'date_records' table showed that the 'holdings' had 2 unique ciks and their latest report dates both were 2020-11-16.

Then, 'update_reports.py' helped in updating the 'holdings' table by comparing submitted dates according to cik. 
For example, I added a new fund Millennium into 'holdings' table and check if there are any recent reports for Two Sigma. 
Renew table 'date_records' by 'dates.py'. And the result returned a three records table that appended Millennium.

Lastly, 'ticker_to_cik.py' documented companies' ciks, tickers and names.


