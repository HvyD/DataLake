# Project: Data Lake


### Summary
This project demonstrates the student's ability to build an ETL pipeline for a data lake hosted on S3.

### Schema of the database
#### Fact table
1. **songplays** - records in log data associated with song plays
  - *user_id, first_name, last_name, gender, level*

#### Dimension tables
2. **users** - users in the app
  - *user_id, first_name, last_name, gender, level* 
3. **songs** - songs in music database
  - *song_id, title, artist_id, year, duration* 
4. **artists** - artists in music database
  - *artist_id, name, location, latitude, longitude* 
5. **time** - timestamps of records in **songplays** broken down into specific units
  - *start_time, hour, day, week, month, year, weekday* 

All the tables has been normalized which increases data integrity as the number of copies of the data has been reduced, which means data only needs to be added or updated in very few places. The schema is a star schema where the fact table, **songplays** is referencing all the dimnension tables.

### Files
- **etl.py** - run this file to extract log and song data from S3, transform it into fact and dimensions tables mentioned above and store them on S3 to be used by data analysts
- **dl.cfg** - enter your AWS credentials in this file