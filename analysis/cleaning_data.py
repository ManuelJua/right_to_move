# import logging
import pandas as pd
import datetime
import re
from time import time
import os

#Extracts the next element from the target element in the array. If does not find it returns None
def find_item(row,pattern_to_find):
    try:
        
        return [row[i+1] for i,elem in enumerate(row) if pattern_to_find in elem][0]
    except:
        return None
    
#Function to splits a string columns, clasifies and put values in separate columns inf the patter matches
def split_columns(pattern_to_find:str,
                df_input:pd.Series
                ):
    '''This line splits each line and applies the function find_item() to the resulting array,
    in order to parse each variable from the original coluumn in 'column to a parse' to the target
    column 'output_column_name'.
    As an example, it can take the original column 'property_details' extract the info after the label
    'PROPERTY TYPE' and asign the value to the column 'property_type'.
    The reason for doing it like this is because the variables searched may or may not be included
    in the row
    '''
    df_output=df_input.apply(lambda x: find_item(x.split(','),pattern_to_find))
    return df_output


def main_cleaning():
    dir=os.path.realpath('clean_right_to_move_files')

    #Finds all file names in right_to_move_files
    right_to_move_files=pd.Series(list(os.walk(dir))[0][2])

    #Searches for the lastest date in the directory righ_to_move_files.
    #If the folde is empty, sets the date to '2022-09-30'
    try:
        last_file_date=right_to_move_files.sort_values().str.findall('\d\d\d\d-\d\d-\d\d').iloc[-1][0]
    except:
        print('Folder is empty')
        last_file_date='2022-09-30'
        print(f'Start date is set to {last_file_date}')

    #Adds one more day to the last file date in order to start downloading the next files from s3
    next_start_date=pd.to_datetime(last_file_date).date()+datetime.timedelta(days=1)

    start_date=next_start_date
    finish_date=datetime.date.today()
    delta=datetime.timedelta(days=1)

    start_total=time()
    files_count=0
    while start_date<=finish_date:
        try:
            start=time()
            df=pd.read_csv(f"right_to_move_files/right_to_move-{start_date}.csv")
            files_count+=1
        except:
            #adds one more day to tha file date
            start_date+=delta
            continue
        
        df['date']=str(start_date)
        #Drops duplicated rows based on 'url' and 'description' columns.
        # It also eliminates de column 'other features' as useful info cannot be extracted from it
        df=(
            df.pipe(lambda df:df.drop_duplicates(subset=['description']))
            .pipe(lambda df:df.drop_duplicates(subset=['address']))
            .drop(columns='other_features')
            .dropna()
            .reset_index(drop=True)
        )
        if 'url' in df.columns:
            df.drop(columns='url',inplace=True)
            
        #Cleaning Prices column
        df['price']=(df['price']
        .str.replace("[pcm£,]","",regex=True)
        # .astype('int64')
        )

        
        #Using split_columns function to split 'property details' columns into property type,
        #number of bedrooms and number of bathrooms
        
        df['property_type']=split_columns('PROPERTY TYPE',df['property_details'])
        df['bedrooms']=split_columns('BEDROOMS',df['property_details']).str.replace("[×]","",regex=True)
        df['bathrooms']=split_columns('BATHROOMS',df['property_details']).str.replace("[×]","",regex=True)

        # df['bedrooms']=pd.to_numeric(df['bedrooms'],errors='coerce')
        # df['bathrooms']=pd.to_numeric(df['bathrooms'],errors='coerce')


        #Separating 'letting_details' column into let_available_date, deposit, min_tenancy,
        #let_type, furnish_type
        df['let_available_date']=split_columns('Let available date: ',df['letting_details'])
        df['deposit']=split_columns('Deposit: ',df['letting_details'])
        df['min_tenancy']=split_columns('Min. Tenancy: ',df['letting_details'])
        df['let_type']=split_columns('Let type: ',df['letting_details'])
        df['furnish_type']=split_columns('Furnish type: ',df['letting_details'])

        #Extracting Council Tax Bands through the use of regex. 
        # df['council_tax_band']=(
        #             #It searches for 'band: B' for instance
        #             df['description'].str.findall("band:?\s?-?\s?[A-HX]\r?,",re.IGNORECASE)
        #             #It takes the las 3 characters of the first element if the array is not empty or else returns None 
        #             .apply(lambda x: x[0][-3:-1] if x!=[] and x!=None and isinstance(x,list) else None) 
        #             #It selects only the letter that is valid as concil tax band
        #             .apply(lambda x:re.search("[A-HX]",x).group(0) if x!=None and isinstance(x,str) else None) 
        #             )

        #Extracting Letting Agent Registration Numbers
        df['letting_agent_registration_number']=(
            df['description'].str.findall('LARN\d+',flags=re.IGNORECASE)
            .apply(lambda x: x[0] if len(x)!=0 else pd.NA)
        )
        
        #Extracting Landlord Registration Numbers
        df['landlord_registration_number']=(
                                            df['description'].str.findall('\d{5,}/\d+/\d{5,}')
                                            .apply(lambda x: x[0] if len(x)!=0 else pd.NA)
                                            )

        #Extracting EPC Ratings
        df['EPC_rating']=(df['description']
                        .str.findall('(EPC\W+\w+\W+\w)',flags=re.IGNORECASE)
                        .apply(lambda x:x[0][-1] if len(x)>0 else pd.NA)
                        .str.upper()
                        )

        #Extracting postcode district
        df['postcode_district']=(
                                df['address'].str.findall('EH\d\d?')
                                .apply(lambda x:x[0] if len(x)>0 else pd.NA)
                                )

        #Dropping 'property_details' and 'letting_details' columns
        df.drop(columns=['property_details','letting_details'],inplace=True)

        #Cleaning agencies names
        df['partial_agent_url']=(
                                df['partial_agent_url'].str.split('/')
                                .apply(lambda x: x[3] if len(x)>2 else pd.NA)
                                )
        
        df=df.rename(columns={'partial_agent_url':'agent_name'})

        #Filtering property types
        df.loc[df['property_type']=='Apartment','property_type']='Flat'
        df.loc[df['property_type']=='Ground Flat','property_type']='Flat'

        #saving clean file to csv
        df.to_csv(f"clean_right_to_move_files/clean_{start_date}.csv",index=False)
        print(f"File clean_right_to_move_files/clean_{start_date}.csv cleaned and saved")
        #adds one more day to tha file date
        start_date+=delta

        end=time()
    end_total=time()
    if files_count>0:
        print(f"{files_count} files processed. Average time per file: {(end_total-start_total)/files_count}")
    else:
        print('No files cleaned')



#Runs the script
main_cleaning()