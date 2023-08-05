#create a table in RDS for exporting incremental data to S3
create external table airline 
(fl_date varchar(50), op_carrier varchar(50), op_carrier_fl_num varchar(50),
origin varchar(50), dest varchar(50), crs_dep_time varchar(50),
dep_time varchar(50), dep_delay varchar(50), taxi_out varchar(50), wheels_off varchar(50), 
wheels_on varchar(50), taxi_in varchar(50), crs_arr_time varchar(50), arr_time varchar(50),
arr_delay varchar(50), cancelled varchar(50), cancellation_code varchar(50),
diverted varchar(50), crs_elapsed_time varchar(50), actual_elapsed_time varchar(50),
air_time varchar(50), distance varchar(50), carrier_delay varchar(50),
weather_delay varchar(50), nas_delay varchar(50), security_delay varchar(50),
late_aircraft_delay varchar(50), id varchar(50) );