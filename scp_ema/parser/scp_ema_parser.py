#!/bin/python3

"""
About this Class

We want to execute the following actions on the SCP EMA data:
      * Parse subject-level pings
      * Aggregate into a master CSV
      * Gunzip all files together
      * Delete directory tree

We'll then download the resulting files for the end user

Ian Richard Ferguson | Stanford University
"""

# --- Imports
from datetime import datetime
import os, pathlib, json, sys, tarfile
import shutil
from tqdm import tqdm
import pandas as pd
import numpy as np
from time import sleep


# --- Class definition
class EMA_Parser:

      def __init__(self, filename, output_path):
            self.filename = filename
            self.output_path = output_path

            output = self._output_directories()
            self.subject_output = output[0]
            self.aggregate_output = output[1]


      def _output_directories(self):
            """
            
            """

            output = []

            for subdir in ["Subjects", f"SCP-EMA_Output"]:

                  temp = os.path.join(self.output_path, subdir)

                  if not os.path.exists(temp):
                        pathlib.Path(temp).mkdir(exist_ok=True, parents=True)
                        output.append(temp)

            return output


      # -- Sanity checks
      def sanity_check(self, JSON, OUTPUT_DIR):
            """
            
            """

            # Read in subject data JSON as dictionary
            with open(os.path.join(self.output_path, self.filename)) as incoming:
                  data = json.load(incoming)

            keys = list(data.keys())                                                # List of keys from JSON
            sub_ids = set([x.split('-')[0] for x in keys])                          # Unique subject IDs

            output_dict = {}                                                        # Empty dictionary to append into

            print("\nIdentifying duplicate subject responses...\n")

            for sub in tqdm(sub_ids):
                  instances = [x for x in keys if sub in x]                         # Number of responses from single sub

                  if len(instances) > 1:                                            # Add to output dict if multiples exist
                        output_dict[sub] = {}
                        output_dict[sub]['count'] = len(instances)
                        output_dict[sub]['keys'] = instances

            print("\nSaving response-duplicates JSON file...\n")

            # Push to local JSON file
            with open(os.path.join(self.aggregate_output, "response-duplicates.json"), "w") as outgoing:
                  json.dump(output_dict, outgoing, indent=4)



      # -- Pings
      def derive_pings(self, SUBSET, KEY):
            """
            SUBSET => Reduced dictionary containing 
            KEY => Key from the master JSON
            This function isolates ping data from the participant's dictionary
            Returns wide DataFrame object with select columns
            """

            pings = pd.DataFrame(SUBSET['pings'])                                   # Convert JSON to DataFrame
            pings['username'] = KEY.split('-')[0]                                   # Add username column
            
            login_node = KEY.split('-')[1:]
            login_node = "".join(login_node)

            pings['login-node'] = login_node

            return pings.loc[:, ['username', 'login-node', 'streamName', 'startTime', 
                                    'notificationTime', 'endTime', 'id', 'tzOffset']]


      # -- Answers
      def derive_answers(self, SUBSET, LOG, USER):
            """
            SUBSET => Reduced dictionary of subject information (pings/user/answers)
            LOG => Text file to log issues
            USER => Username, used in error log
            This function isolates participant respones and converts from long to wide
            Returns DataFrame object
            """

            def isolate_values(DF):
                  """
                  DF => Dataframe object
                  While data is still "long", we'll isolate the participant response
                  """

                  if DF['preferNotToAnswer']:
                        return "PNA"

                  try:
                        # Raw data is optimized for dictionary expresson, we'll save the values
                        temp = dict(DF['data']).values()
                        return list(temp)

                  except:
                        # NOTE: Consider returning empty string instead
                        return None

            # Isolated participant response dictionary
            answers = pd.DataFrame(SUBSET['answers'])

            try:
                  # Create new "value" column with aggregated response
                  answers['value'] = answers.apply(isolate_values, axis=1)

            except Exception as e:
                  # Write to error log
                  LOG.write(f"\nCaught @ {USER} + isolate_values: {e}\n\n")

            try:
                  # Apply cleanup_values function (removes extra characters)
                  answers['value'] = answers['value'].apply(lambda x: self.cleanup_values(x))

            except Exception as e:
                  # Write to error log
                  LOG.write(f"\nCaught @ {USER} + cleanup_values: {e}\n\n")

            answers = answers.drop_duplicates(subset="date", keep="first").reset_index(drop=True)

            answers["IX"] = answers.groupby("questionId", as_index=False).cumcount()

            # Drop extraneous columns
            answers.drop(columns=['data', 'preferNotToAnswer'], inplace=True)

            # Pivot long to wide
            answers = answers.pivot(index="pingId", columns="questionId", values="value").reset_index()

            # Rename Ping ID column (for merge with pings DF)
            answers.rename(columns={'pingId':'id'}, inplace=True)

            return answers



      def cleanup_values(self, x):
            """
            x => Isolated value derived from lambda
            This function is applied via lambda, serialized per column
            """

            # Parse out parentheses
            #temp = str(x).replace('(', '').replace(')', '')
            temp = str(x)

            """
            The conditional statements below will strip out square brackets
            and leading / trailing parentheses
            Yields a clean value to work with in the resulting dataframe
            """

            if temp[0] == "[":
                  temp = temp[1:]

            if temp[-1] == "]":
                  temp = temp[:-1]

            if temp[0] == "\'":
                  temp = temp[1:]
            elif temp[0] == "\"":
                  temp = temp[1:]

            if temp[-1] == "\'":
                  temp = temp[:-1]
            elif temp[-1] == "\"":
                  temp = temp[:-1]

            return temp



      def parse_nominations(self, DF):
            """
            DF => Dataframe object
            This function is named nominations ... e.g., Dean Baltiansky
            The following columns are parsed...
                  * SU_Nom => 1,2,3
                  * SU_Nom_None_Nom => 1,2,3
                  * NSU_Rel => 1,2,3
                  * NSU_Nom_None_Nom => 1,2,3
            """

            # Keys are existing columns, Values are new columns
            voi = {'SU_Nom': 'SU_Nom_{}',
                  'SU_Nom_None_Nom': 'SU_Nom_None_Nom_{}',
                  'NSU_Rel': 'NSU{}_Rel',
                  'NSU_Nom_None_Nom': 'NSU{}_None_Rel'}

            for parent in list(voi.keys()):                                      

                  # Not every participant has every variable ... this will standardize it
                  if parent not in list(DF.columns):
                        DF[parent] = [] * len(DF)
                        continue

                  for k in [1, 2, 3]:                                                 
                        new_var = voi[parent].format(k)                                 # E.g., SU_Nom_1
                        DF[new_var] = [''] * len(DF)                                    # Create empty column

                  for ix, value in enumerate(DF[parent]):                       
                        try:
                              check_nan = np.isnan(value)                                 # If value is null we'll skip over it
                              continue
                        except:
                              if str(value) == "None":                                    # Skip over "None" and "PNA" values
                                    continue
                              elif value == "PNA":
                                    continue

                        value = value.replace("\"", "\'").split("\',")                  # Replace double-quotes, split on comma b/w nominees

                        for k in range(len(value)):                                     
                              new_var = voi[parent].format(k+1)

                        try:                           
                              new_val = value[k]                                      # Isolate nominee by list position
                        except:
                              continue                                                # Skip over index error

                        for char in ["[", "]"]:
                              new_val = new_val.replace(char, "")                     # Strip out square brackets

                        new_val = new_val.strip()                                   # Remove leading / trailing space

                        DF.loc[ix, new_var] = new_val                               # Push isolated nominee to DF

            for parent in list(voi.keys()):
                  for k in [1,2,3]:
                        new_var = voi[parent].format(k)

                        # Run cleanup_values function again to strip out leading / trailing characters (for roster matching)
                        DF[new_var] = DF[new_var].apply(lambda x: self.cleanup_values(x))

            return DF


      def parse_race(self, DF):
            """
            DF => DataFrame object
            This function un-nests race responses
            Returns list of all responses marked True (may be more than one)
            """

            def isolate_race_value(x):
                  """
                  x => Isolated value derived from lambda
                  This function will be applied via lambda function
                  Returns list of True values
                  """

                  try:
                        # Strip out all quotes
                        temp = x.replace("\"", "").replace("\'", "")

                        # Split on category and isolate responses that were marked true
                        race_vals = [k.split(',')[0]
                                    for k in temp.split('],') if "True" in k]

                        # Strip out square brackets
                        race_vals = [k.strip().replace('[', '').replace(']', '')
                                    for k in race_vals]

                        return race_vals

                  except:
                        # In the case of missing data
                        return None

            try:
                  # Make sure Race column is present
                  check = list(DF['Race'])

                  # Apply isolate_race_value helper function
                  DF['Race'] = DF['Race'].apply(lambda x: isolate_race_value(x))

            except:
                  # If key doesn't exist, create empty column
                  DF['Race'] = [] * len(DF)

            return DF


      # --- Concat
      def agg_drop_duplicates(self, DF):
            """
            NOTE: This helper is functional but not in use
            """

            users = list(DF['username'].unique())

            keepers = []

            for user in tqdm(users):
                  temp = DF[DF['username'] == user].reset_index(drop=True)
                  temp = temp.drop_duplicates(subset="id", keep="first").reset_index(drop=True)
                  keepers.append(temp)

            return pd.concat(keepers)


      # --- Devices

      def parse_device_info(self, SUBSET, KEY):
            """
            SUBSET => Particpant's reduced JSON file (as Python dictionary)
            KEY => Key from the JSON data dictionary
            This function flattens user device info into a single-row
            DataFrame object. This is returned and stacked with others
            in the main function
            Returns DataFrame object
            """

            devices = SUBSET['user']                                            # Isolate device information from JSON
            username = devices['username']                                      # Pull in username from data dictionary
            login_time = KEY.split('-')[-1]                                     # Isolate subject login time from data key


            master = pd.DataFrame({'username':username}, index=[0])             # Parent DF to merge into

            for key in ['device', 'app']:
                  temp = devices['installation'][key]                             # Isloate sub-keys from dictionary
                  temp_frame = pd.DataFrame(temp, index=[0])                      # Flatten wide to long
                  temp_frame['username'] = username                               # Isolate username
                  temp_frame['login_time'] = login_time                           # JavaScript derived login time
                  
                  master = master.merge(temp_frame, on='username')                # Merge with parent DF on username

            return master


      # --- Run
      def parse_responses(self, KEY, SUBSET, LOG, OUTPUT_DIR, KICKOUT):
            """
            KEY => Key from the master data dictionary
            SUBSET => Reduced dictionary of participant-only data
            LOG => Text file to store errors and exceptions
            OUTPUT_DIR => Relative path to output directory
            KICKOUT => Boolean, if True a local CSV is saved
            This function wraps everything defined above
            Returns a clean DataFrame object
            """

            username = KEY.split('-')[0]                                            # Isolate username

            try:
                  answers = self.derive_answers(SUBSET=SUBSET, LOG=LOG, USER=username)     # Create answers DataFrame
            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + derive_answers: {e}\n\n")

            try:
                  answers = self.parse_race(answers)                                       # Isolate race responses
            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + parse_race: {e}\n\n")

            try:
                  answers = self.parse_nominations(answers)                                # Isolate nomination responses
            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + parse_nominations: {e}\n\n")

            try:
                  pings = self.derive_pings(SUBSET=SUBSET, KEY=KEY)                        # Create pings DataFrame
            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + derive_pings: {e}\n\n")

            # Isolate a few device parameters to include in pings CSV
            # The exhaustive device info is in another CSV in the same directory
            devices = pd.DataFrame(SUBSET['user']['installation']['device'], index=[0])
            devices['username'] = username
            pings = pings.merge(devices, on="username")

            return self.output(KEY, pings, answers, OUTPUT_DIR, KICKOUT)



      def output(self, KEY, PINGS, ANSWERS, OUTPUT_DIR, KICKOUT):
            """
            KEY => Key from JSON file
            PINGS => Pandas DataFrame object
            ANSWERS => Pandas DataFrame object
            OUTPUT_DIR => Relative path to aggregates directory
            KICKOUT => Boolean, determiens if CSV will be saved
            Merges pings and answers dataframes
            Returns DataFrame object
            """

            # Isolate username
            KEY = KEY.split('-')[0]

            # Combine dataframes on ping identifier (e.g., modalStream1)
            composite_dataframe = PINGS.merge(ANSWERS, on="id")

            # Option to save locally or not
            if KICKOUT:
                  output_name = os.path.join(f"{OUTPUT_DIR}/{KEY}.csv")

                  # Avoid duplicates (possible with same username / different login IDs)
                  if os.path.exists(output_name):
                        output_name = os.path.join(f"{OUTPUT_DIR}/{KEY}_b.csv")

                  composite_dataframe.to_csv(output_name, index=False, encoding="utf-8-sig")

            return composite_dataframe



      def run_this_puppy(self):
            """
            
            """

            target_path = self.output_path
            sub_data, output_filename = self.filename, self.filename.split('.json')[0]

            # These output directories will hold parsed data
            subject_output_directory = self.subject_output
            aggregate_output_directory = self.aggregate_output

            self.sanity_check(sub_data, aggregate_output_directory)

            # I/O JSON file
            with open(os.path.join(self.output_path, self.filename)) as incoming:

                  print(f"\n\nParsing {os.path.join(self.output_path, self.filename)}\n\n")

                  # I/O new text file for exception logging
                  with open(f"{target_path}/{output_filename}.txt", "w") as log:

                        # Read JSON as Python dictionary
                        data = json.load(incoming)

                        # Empty list to append subject data into
                        keepers = []
                        
                        # Empty dictionary to append sparse data into
                        parent_errors = {}

                        print("\nParsing participant data...\n")
                        sleep(1)

                        # Key == Subject and login ID (we'll separate these later)
                        for key in tqdm(list(data.keys())):

                              # Reduced data for one participant
                              subset = data[key]

                              # If participant completed no pings, push them to parent dict
                              if len(subset['answers']) == 0:
                                    parent_errors[key] = subset
                                    continue

                              try:
                                    # Run parse_responses function to isolate participant data
                                    parsed_data = self.parse_responses(key, subset, 
                                                                       log, subject_output_directory, True)

                              except Exception as e:
                                    # Catch exceptions as they occur
                                    log.write(f"\nCaught @ {key.split('-')[0]}: {e}\n\n")
                                    continue

                              # Add participant DF to keepers list
                              keepers.append(parsed_data)

                        sleep(1)
                        print("\nAggregating participant data...\n")

                        try:
                              # Stack all DFs into one
                              aggregate = pd.concat(keepers)

                              # Push to local CSV
                              aggregate.to_csv(f'{self.aggregate_output}/pings_{output_filename}.csv',
                                                index=False, encoding="utf-8-sig")

                        except Exception as e:
                              # Something has gone wrong here and you have no participant data ... check the log
                              print(f"{e}")
                              print("\nNo objects to concatenate...\n")
                              sys.exit(1)

                        print("\nSaving parent errors...\n")

                        # Push parent errors (no pings) to local JSON
                        with open(f'{self.aggregate_output}/parent-errors.json', 'w') as outgoing:
                              json.dump(parent_errors, outgoing, indent=4)

                        print("\nParsing device information...\n")

                        # I/O new text file for device parsing errors
                        with open(f"{self.aggregate_output}/device-error-log.txt", 'w') as log:

                              device_output = []                                    # Empty list to append into

                              # Same process as before, we'll loop through each subject
                              for key in tqdm(list(data.keys())):

                                    username = key.split('-')[0]                    # Isolate username from key naming convention

                                    try:
                                          # Isolate participant dictionary
                                          subset = data[key]
                                          device_output.append(self.parse_device_info(subset, key))

                                    except Exception as e:
                                          # Catch exceptions as they occur
                                          log.write(f"\nCaught {username} @ device parser: {e}\n\n")

                                    # Stack participant device info into one DF
                                    devices = pd.concat(device_output)

                                    # Push to local CSV
                                    devices.to_csv(f'{self.aggregate_output}/devices_{output_filename}.csv',
                                                index=False, encoding="utf-8-sig")

                              sleep(1)
                              print("\nAll responses + devices parsed\n")


      def gunzip_output(self):
            """
            
            """

            filename = datetime.now().strftime("%b_%d_%Y")

            with tarfile.open(f"{self.output_path}/SCP_EMA_Responses.tar.gz", "w:gz") as tar:
                  tar.add(self.aggregate_output, arcname=f"SCP_EMA_Responses_{filename}")



      def big_dogs_only(self):
            """
            
            """

            self.run_this_puppy()
            self.gunzip_output()