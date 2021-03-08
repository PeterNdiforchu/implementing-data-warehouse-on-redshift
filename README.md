# Implementing Data Warehouse On Redshift

>The goal of this project is to extract raw data stored in an S3 Bucket and load them unto staging tables on Redshift 
>for transformation through SQL queries executed on them to create final analytical tables.

## Table of contents

* [General info](#general-info)
* [Screenshots](#screenshots)
* [Technologies and Libraries](#technologies-and-libraries)
* [Project Setup](#project-setup)
* [Quality Assurance](#quality-assurance)
* [Contact](#contact)

## General info
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. To better serve customers the start-up wants to better understand how users interact with the app and there's no better way to do it at scale than applying dimensional modelling best practises in a data warehouse.

## Screenshots
Fig.1: Sample Staging_songs table on redshift
![staging_songs_table_ss_redshift](https://user-images.githubusercontent.com/76578061/110278526-a41a3380-7f94-11eb-9dd2-4704831ffc5c.png)

Fig.2: Sample Staging_events table on redshift
![staging_events_table_se_redshift](https://user-images.githubusercontent.com/76578061/110278709-fd826280-7f94-11eb-8f21-414ebe2dd504.png)
