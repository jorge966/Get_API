from flask import Flask, request
from urllib.parse import quote
import requests
import json
import pprint
from operator import itemgetter
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time

app = Flask(__name__)

@app.route('/api/ping', methods=['GET'])
def ping():
    """
    GET Request that returns a response with "success": "true" and status code 200
    """
    return { "success": "true"}, 200

@app.route('/api/posts', methods=['GET'])
def posts():
    """
    GET Request that takes in 3 parameters:
    param1: tags (comma separated values)
    param2: sortBy
    param3: direction
    returns: a dictionary object that contains a list of sorted post dictionaries
    """

    # Get all comma separated tags from querystring
    tags_param = request.args.get('tags').split(',')
    # Ensure that tags param is not empty, if it is throw a 400 error.
    if tags_param == None:
        return { "error": "Tags parameter is required" }, 400

    # Create list of valid sortBy values
    valid_sortBy = ['id', 'likes', 'popularity', 'reads']
    # Get the sortBy parameter from querystring
    sortBy_param = request.args.get('sortBy')
    # Check if the sortBy value is empty, if it is set it to default value: 'id'
    if sortBy_param == None:
        sortBy_param = 'id'
    # If the sortBy value is not a part of the valid_sortBy list, throw a 400 error
    if sortBy_param not in valid_sortBy:
        return { "error": "Invalid sortBy parameter was passed into the query" }, 400

    # Create list of valid direction values
    valid_direction = ['asc', 'dsc']
    # Get the direction parameter from querystring
    direction_param = request.args.get('direction')
    # Check if the direction value is empty, if it is set it to the default value: 'asc'
    if direction_param == None:
        direction_param = 'asc'
    # If the direction value is not a part of the valid_direction list, throw a 400 error
    if direction_param not in valid_direction:
        return { "error": "Invalid direction parameter was passed into the query" }, 400

    # Create a master list of all posts of given tags
    master_list = []
    # For each specified tag, retrieve all related posts from the API service
    for tag in tags_param:
        try:
            # Add all retrieved posts to the master_list
            for item in data_source(tag):
                master_list.append(item)
        except:
            # If the user passed an invalid tag that returns nothing, throw an error
            return { "error": "Either no posts were retrieved, or the Tag: " + tag + " is an invalid tag parameter which was passed into the query" }, 406

    # Removes duplicate ID's from the master_list
    master_list = remove_duplicate_posts(master_list)

    # Sorts the final master_list based on the sortBy and direction parameters
    master_list = sort_data_by_filter(master_list, sortBy_param, direction_param)

    # Returns the final master_list
    return { "posts": master_list }, 200

### Helper Functions

# Gets all posts with the corresponding tag from the API service
def data_source(tag) -> list:
    # Create a URL safe string
    qstr = quote(tag)
    # Create the request URL for the specified tag
    url = "https://hatchways.io/api/assessment/blog/posts?tag=" + qstr
    looper = loop.run_until_complete(Get_request(url)).json()

    # Convert response to a JSON object
    #response = requests.get(url).json()
    # Return the ONLY the list of posts
    return looper

# Sorts all posts in a given list by specified sortBy and direction parameters
def sort_data_by_filter(data, sortBy, direction) -> list:
    if direction == "asc":
        # Return a sorted list through the sortBy key:value pairs
        return sorted(data, key=itemgetter(sortBy))
    else:
        # Return a reversed sorted list through the sortBy key:value pairs
        return sorted(data, key=itemgetter(sortBy), reverse=True)

# Removes duplicate posts by ID
def remove_duplicate_posts(data_list) -> list:
    # Create an empty set for the post IDs
    seen = set()
    # Create an empty return list
    return_list = []
    # Iterate through each post from the data_list
    for data in data_list:
        # Check if the post ID is not in the seen set
        if data['id'] not in seen:
            # If the id is not in the set, add it to the set
            seen.add(data['id'])
            # Add the post to the return list, if it is not a duplicate
            return_list.append(data)
    return return_list

executer = ThreadPoolExecutor()
loop = asyncio.get_event_loop()

async  def  Get_request(url):
    futures = [loop.run_in_executor(executer,requests.get, url)]
    await asyncio.wait(futures)




app.run()
