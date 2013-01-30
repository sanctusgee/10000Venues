import io
import os
import codecs
import fileinput

FILE_NAME= "VenueList.txt"
DEST_FILE = "Updated-VenueList.json"

def set_credentials():
   '''initialize the appp OAuth key and secret '''
   
   KEY = "<YOUR FACTUAL API KEY"
   SECRET = "<YOUR FACTUAL API SECRET>"
   cred = Factual(KEY, SECRET)

   return cred
#of course, RegEx works here as well...
def find_replace(line_item):
    ''' check repl{old_value : new_value} and 
         replace old_value in string with new_value   '''
    repl = {
            #use the Factual Id as the primary key (_id):
            "[," : "[",
            "factual_id" : "rec_id",
           "'": '"',
           'u"': '"',          

           ": False,": ': "False",',
           ": False": ': "False"',

            ": True,": ': "True",',
            ": True": ': "True"',
            #remove the source file square brackets before/after each city name 
            # each json document brings its own "[]"
            "[{" : "{",
            "}]" : "}"                             
         }        

    for old_value, new_value in repl.iteritems():                    
      line_item = str(line_item).replace(old_value, new_value)        
    
    return line_item     

def add_json_header(input_file):
    '''add the json header and footer to the file'''
    with codecs.open(input_file, 'r+', 'utf-8') as f:
        content = f.read()        
        #add the json header at beginning of file:
        f.seek(0, 0)              
        f.write('{ "venues": [' + content)
   
        #now go to end of file and insert closing braces
        f.seek(0, 2)       
        f.write("]}")
        f.close()      
      
    return input_file


def transform_resultset(source_file):
   '''replace occurence of values in a string'''        
    
    #replace the "u'" character, enclose True and False in quotes, etc, push updated data to new file
   try:
       #todo: check if file exists, then delete b4 proceeding with below      
       with codecs.open(DEST_FILE, 'wb', 'utf-8') as dest_file:               
         for line in fileinput.input(source_file):          
               line = find_replace(line)                
               dest_file.write(str(line))           
   except Exception as err:
      dest_file.close()
      print("string replace failed  -  {}".format(err))
   finally:
         #clean up after urself, kid
         dest_file.close()    
         print("\nALL CITIES:\nstring replace completed")      
         #os.remove(source_file)       


def extract_to_staging(stage_file, result_set, city):
       '''output the extracted results to a csv/tct file'''       
       try:           
           #write the results to file
           with codecs.open(stage_file, 'ab', 'utf-8') as f:                  
               f.write(',{}'.format(str(result_set)))
               print("{}:\nplaces upload complete\n".format(city.upper()))
       except Exception as err:
           print("could not write to file")
           print("Error:  {}".format(err))           
       finally:
           f.close()
     
       return stage_file                          
       
def run_search():
   factual = set_credentials()   
   stage_file = FILE_NAME      
   
   #table = factual.table('places')
   restaurants = factual.table("restaurants-us")      
   city_list = ["New York", "Los Angeles", "Chicago", "Houston", "Philadelphia"
                     ,"Phoenix", "San Anotnio", "San Diego", "Dallas", "San Jose"
                     ,"Jacksonville", "Indianapolis", "Austin", "San Francisco"
                     ,"Columbus", "Fort Worth", "Charlotte", "Detroit", "El Paso"
                     ,"Memphis", "Boston", "Seattle", "Denver", "Baltimore"
                     #.... other cities
                ]
   
    venues = {}
   
   for cities in city_list:
      #restaurant in given city, with star rating of 4 or greater,
      #has wifi access, with name and cuisine listed
       search = restaurants.filters(
                                {"$and":[
                                         {"locality":"{}".format(cities)},
                                         {"rating":{"$gte":4}},
                                         {"wifi":"true"},
                                         {"name": {"$blank":False}},
                                         {"cuisine": {"$blank":False}}                                         
                                         ]
                                 }).limit(1)
       
       #explicitly specify the fields to return
       search = search.select("rating, tel, postcode, parking, seating_outdoor, category, cuisine, locality,"
                                 "payment_cashonly,latitude, website, price, factual_id, address, open_24hrs,"
                                 "name, country, region, wifi,accessible_wheelchair, longitude, status")
       venues[cities] = search.data()     
       #write results to csv file  - MULTIPLE times
       src_file = extract_to_staging(stage_file, venues[cities], cities)
   #end for        
      
   return src_file

def main():   
   src_file = run_search()
   # add json header /footer to result-set
   new_file = add_json_header(src_file)      
   # clean up the data - remove ascii characters
   transform_resultset(new_file)
   
   raw_input()


if __name__ == '__main__':
   main()
