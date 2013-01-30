#convert .csv files to json format

import csv
import json
import codecs

def convert_to_json():
    f = open( 'metro_stats.csv', 'r' )
    reader = csv.DictReader( f, fieldnames = ( "Rank","City", "State","2010 Census", "2010 Census Final",
                                              "2010 Census Estimate", "2011 Estimate" ) )
    with codecs.open("metro_stats.json", 'w', 'utf-8') as temp:
        #add the json header at beginning of file
        temp.write( '{ "meta": {"code": 200 },"response": {"cityStats":')
        out = json.dumps( [ row for row in reader ] )
        temp.write(out)
      
        #go to end of file and write closing braces
        temp.seek(0, 2)
        temp.write("}} ")             
        temp.close()

    
    raw_input()
