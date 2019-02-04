# Spark
Create a dataset of about 700 MB with the following schema:

taxi_driver_name: String (Name of the driver),
pickup_loc: String (Location from where the passenger was picked (Area)),
pickup_ts: Timestamp (Epoch Timestamp of pickup), 
drop_loc:String (Location from where the passenger was dropped (Area)),
drop_ts: Timestamp (Epoch Timestamp of drop),
no_of_passengers: int (Count of passengers picked up),
fare: double (Amount charged for the ride),
city: String,
state: String,
country: String,
weather: String (Can be cloudy, windy, rainy or clear),
customer_review: int (Review can be between 0 to 5, 0 being bad and 5 being good).

Usecase: With the above given schema generate the data and do the following for analysis and reporting  for 'Vroom Vrooom' Company.

Reporting: Find how monthly sales for each city.
Reporting: Find out top 10 most charged taxi rides for each city.
Reporting: Country wise count the number of taxi rides, order them from highest to lowest.
Reporting: Find out top 10 taxi drives who took the most number of passengers per city.
Analytical: Company Vroom Vrooom wants to know hourly which areas are the busiest in terms of taxi rides.
Analytical: Company wants to understand in which weather their taxi sales are more.
Analytical: In each city which areas take the most time to commute.
Analytical: For their yearly reward which is given city wise they want to choose 10 taxi drivers per city that serve the best. Find a list of those.
Analytical: With your help Vroom Vrooom wants to understand in which weather their sales are the least so that they can design a solution to improve them. 
Note: Write all output to a text file.
