
# TripHawk

This is the backbone of the big data management part of the project. For this we are creating data pipelines where we input data from API calls and other data sources and finally store the data in a distributed database.

For this project we are using Python as our main language to create the data pipelines. The data collection is done with python scripts and the requests module.

Crontab module is used to create the scheduler, the one that will call the API’s and store the data on a certain period of time.

HDFS is used as a distributed file system where all the data is being stored after being fetched.

MongoDB is used as a distributed NoSQL database which is being fed with the data from the API calls, stored in HDFS.

  

### Tools Required

  

In order to run this project you need the following tools:

  

* Python3

* MongoDB server

* HDFS

* API's from:

- [Yelp Fusion API]

- [Foursquare Places API]

* Preferably a VM to run the code via SSH.

  

### Installation
First, make sure you install all the required modules from `requirements.txt`.


In order to be able to run and call the API, you need an API key that you can get from Foursquare Places and Yelp Fusion. The dictionary with keys should be put in `credentials/keys.py`. This folder is ignored by git. 

Both HDFS and MongoDB should be started and running on the machine. They need to run in the background. Make sure that you change the `HOST:PORT` in both files in `scripts/temp_loaders` directory as well as for MongoDB in `scripts/perm_loaders/mongo.py`

While testing we were using the UPC's virtual machines: virtech.fib.upc.edu/

There are multiple ways you can fetch and store data.
1. Run crontab and each data pipeline will run every certain period of time. In order to do this, simply run `python3 scripts/scheduler.py`. To check if the demons are running do `$ crontab -l`. In case you want to change the intervals do `$ crontab -e`
2. You can run the pipelines manually using: `python3 scripts/pipelines/*`. There you can choose between accommodation, attractions, businesses. 

Running the pipelines will make the data to be fetched/scraped from the sources, uploaded to HDFS, read from HDFS and inserted in MongoDB. 
  

## Authors

  

#### Filip Sotiroski

[GitHub Filip]

#### Zhicheng Luo

[GitHub Zhicheng]

  

[//]: #  (HyperLinks)

  

[GitHub Filip]: https://github.com/madhur-taneja

[GitHub Zhicheng]: https://github.com/ZhichengLuo

[Foursquare Places API]: https://developer.foursquare.com/docs/places-api-overview

[Yelp Fusion API]: https://fusion.yelp.com/
