
#coding: utf-8
# ----------------------------------------------------------------------------
# pyprocessing
# Copyright (c) 2013 Godwin Effiong
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions 
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright 
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#  * Neither the name of pyprocessing nor the names of its
#    contributors may be used to endorse or promote products
#    derived from this software without specific prior written
#    permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# ----------------------------------------------------------------------------
"""

Other than Python, the only requirement is the Factual API
"""

'''
##******************************************************##
##******************************************************##
##                                                      ##
##          CityVenues.py           
##         
##           CREATED BY: Godwin Effiong
##            DATE: Jan 02, 2013
##                                           
##             PURPOSE: Pull City Venues (Places) from Factual
##             Version: 0.8 (alpha)
##                          
##                                                      ##
##******************************************************##
##******************************************************##
'''

import io
import os
import time
import codecs
import fileinput
import pymongo
import fnmatch
from pymongo import MongoClient
from factual import *

FILE_NAME= "VenueList.txt"
DEST_FILE = "Updated-VenueList.txt"

def db_connect():
    connection = MongoClient()
    conection = MongoClient('ds045897.mongolab.com', 45897)
    db = connection['10000venues']
    connection.close()

def set_credentials():
   '''initialize the appp OAuth key and secret '''
   
   KEY = "My_FACTUAL_API_KEY"
   SECRET = "My_FACTUAL_API_SECRET"
   cred = Factual(KEY, SECRET)

   return cred

def find_replace(line_item):
    ''' check repl{old_value : new_value} and 
         replace old_value in string with new_value   '''
    repl = { 
           
           "'": '"',
           'u"': '"',

           ": False,": ': "False",',
           ": False": ': "False"',

            ": True,": ': "True",',
            ": True": ': "True"',
            #remove the source file square brackets before/after each city name 
            # each json document brings its own "[]"
            "[{" : "{",
            "}]" : "}",
            '{ "meta": {"code": 200 },"response": { "venues": [,' 
                  : '{ "meta": {"code": 200 },"response": { "venues": ['
         }        
    for old_value, new_value in repl.iteritems():                    
      line_item = str(line_item).replace(old_value, new_value)   
    
    return line_item     

def add_json_header(source_file):
    '''add the json header and footer to the file'''
    with codecs.open(source_file, 'r+', 'utf-8') as f:
        content = f.read()
        #add the json header at beginning of file:
        f.seek(0, 0)
        f.write('{ "meta": {"code": 200 },"response": { "venues": ['+ content)        
   
        #now go to end of file and insert closing braces
        f.seek(0, 2)
        f.write("]}} ")
        f.close()
        print("\nstring replace completed")
      
    return source_file


def transform_resultset(source_file):
   '''replace occurence of values in a string'''        
    
    #replace the "u'" character, enclose True and False in quotes, etc, push updated data to new file
   try:
       #todo: check if file exists, then delete b4 proceeding with below
      #use append to get all data (mode = 'a')
       with codecs.open(DEST_FILE, 'a', 'utf-8') as dest_file:               
         for line in fileinput.input(source_file):
          #  if not fnmatch.fnmatch(line, "'hours*}"): #u'hours': u'{"1":[["6:00","17:30"]],"2":[["6:00","17:30"]],"3":[["6:00","17:30"]],"4":[["6:00","17:30"]],"5":[["6:00","17:30"]]}
               line = find_replace(line)                
               dest_file.write(str(line))
               dest_file.close()
   except Exception as err:
      dest_file.close()
      print("string replace failed  -  {}".format(err))
   finally:
         #clean up after urslef, kid
         dest_file.close()    
         #os.remove(source_file)       

def extract_to_staging(stage_file, result_set, city):
       '''output the extracted results to a csv/tct file'''       
       try:           
           #write the results to file
           with codecs.open(stage_file, 'a', 'utf-8') as f:               
               f.write(', {}'.format(str(result_set)))
               print("{}:\nplaces upload complete\n".format(city.upper()))
       except Exception as err:
           print("could not write to file")
           print("Error:   {}".format(err))           
       finally:
           f.close()
     
       return stage_file
                          
       
def main():   
   factual = set_credentials()   
   stage_file = FILE_NAME      
   
   #table = factual.table('places')
   restaurants = factual.table("restaurants-us")      
   #city_list = ["New York", "Los Angeles", "Chicago", "Houston", "Philadelphia"
   #                  ,"Phoenix", "San Anotnio", "San Diego", "Dallas", "San Jose"
   #                  ,"Jacksonville", "Indianapolis", "Austin", "San Francisco"
   #                  ,"Columbus", "Fort Worth", "Charlotte", "Detroit", "El Paso"
   #                  ,"Memphis", "Boston", "Seattle", "Denver", "Baltimore"
   #             ]
   city_list = {"Chicago":0, "Boston":1}
   venues = {}

   for cities in city_list.iterkeys():
       search = restaurants.filters(
                                {"$and":[
                                         {"locality":"{}".format(cities)},
                                         {"rating":{"$gte":4}},
                                         {"wifi":"true"},
                                         {"name": {"$blank":False}},
                                         {"cuisine": {"$blank":False}}                                         
                                         ]
                                 }).limit(1)
       venues[cities] = search.data()  
       #write results to csv file  - MULTIPLE times
       src_file = extract_to_staging(stage_file, venues[cities], cities)
   #end for
       #for line in venues[cities][0]['hours']:
       #  if line == 'hours':
        # print(line)
   # 1 - now add json header /footer - ONE time
   new_file = add_json_header(src_file)      
   # 2 - now clean up the data - remove ascii characters, add json header - ONE time
   transform_resultset(new_file)
   #    
   raw_input()

if __name__ == '__main__':
   main()

# replace string where starts with/like
