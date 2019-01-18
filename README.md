# Foodmart-training
A map reduce training program dealing with a foodmart.

# Version 1
In the foodmart folder, there is a simple map reduce program. The mapper splits the data into weekday and weekend sales. The reducer aggregates the sales data for each product.

# Version 2
In the Partitioner folder, the map reduce program added a partitioner. The mapper filters out data that doesn't need to be passed along. The partitioner sorts the values by year and month so that all the data for for each month of each year are grouped together. The reducer then combines the sales data.
