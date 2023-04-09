from sensor import utils
from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor.exception import SensorException
from sensor.logger import logging
import os, sys
import pandas as pd
import numpy as np


class DataIngestion:

    def __init__(self, data_ingestion_config: config_entity.DataIngestionConfig):
        try:
            self.data_ingestion_config= data_ingestion.config
        except Exception as e:
            raise SensorException(e, sys)

    def data_ingestion(self)->artifact_entity.DataIngestionArtiact:
        try:
            #Exporting collection data as pandas dataframe
            df= pd.DataFrame= utils.get_collection_as_dataframe(
                database_name= self.data_ingestion_config.database_name,
                collection_name= self.data_ingestion_config.collection_name)

            #Save data in feature store
            df.replace(to_replace= "na", value= np.NAN, inplace= True) 
            
            #Create feature store folder
            feature_store_dir= os.path.dirname(self.data_ingestion_config.feature_store_file_path)
            os.makedirs(feature_store_dir, exist_ok= True)

            #save dataframe to feature store folder
            df.to_csv(path_or_buf= self.data_ingestion_config.feature_store_file_path, index=False, header=)

            #split dataset into train and test
            train_df, test_df= train_test_split(df, test_size= self.data_ingestion_config.test_size)

        except Exception as e:
            raise SensorException(e, sys)